# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "docker",
#     "requests",
# ]
# ///
import sys
import requests
import csv
import time
import threading
import subprocess
import docker

# Total RAM limit in bytes (3 GB)
TOTAL_RAM_LIMIT = 7 * 1024 * 1024 * 1024
token_counter  = 0 
MAX_CONTAINER_LIMIT = 100 
# How often (in seconds) to check memory usage when waiting.
CHECK_INTERVAL = 2

# Initialize Docker client
client = docker.from_env()

def get_container_memory_usage(container):
    """Return current memory usage in bytes for a container snapshot."""
    try:
        stats = container.stats(stream=False)
        return stats.get("memory_stats", {}).get("usage", 0)
    except Exception as e:
        print(f"Error getting stats for container {container.name}: {e}")
        return 0

def stream_logs(container, log_file):
    """
    Attach to container logs (stdout and stderr) and write them to a file.
    This function follows the logs until the container stops.
    """
    try:
        with open(log_file, "w") as f:
            for line in container.logs(stream=True, stdout=True, stderr=True, follow=True):
                f.write(line.decode())
                f.flush()
    except Exception as e:
        print(f"Error streaming logs for container {container.name}: {e}")

def evaluate_container(container, email,external_port, timeout=120):
    """
    Polls the container logs until a readiness indicator is found or a timeout occurs.
    If the container doesn't become ready within the timeout, it is stopped and removed.
    Otherwise, runs evaluate.py against the container and then stops/removes it.
    """
    start_time = time.time()
    ready = False
    print(f"Waiting for container {container.name} to become ready...")
    logs = container.logs( stdout=True, stderr=True).decode("utf-8")
    task = "Say Hello Carlton"
    # tru this request 5 times before giving up
    for i in range(5):
        try :
            response = requests.post(f"http://localhost:{external_port}/run", params={"task": task},timeout=20)
            ready = True
            with open("server_start.logs","a") as stl : 
                print("server has started up",email, sep="\t",file=stl)
            
            break 
        except Exception as e :
            
            time.sleep(30)
            continue

    if not ready:
        print(f"Container {container.name} did not become ready within {timeout} seconds. Removing container.")
        with open("server_start.logs","a") as stl :
            print("could not start the server in 5 minutes",email, sep="\t",file=stl)
        try:
            container.stop()
            # container.remove()
        except Exception as e:
            print(f"Error stopping/removing container {container.name}: {e}")
        return

    print(f"Starting evaluation for container {container.name}")
    global token_counter
    cmd = ["uv","run", "evaluate.py","--email",f"{email}","--external_port",f"{external_port}","--token_counter",f"{token_counter}"]
    # result = subprocess.run(cmd, capture_output=True, text=True)
    # cmd = ["python", "evaluate.py", container.name]
    log_file_path = f"x86_evaluation_logs/{email}_evaluation.log"

    # Open the log file and redirect both stdout and stderr to it
    with open(log_file_path, "a") as log_file:
        result = subprocess.run(cmd, stdout=log_file, stderr=log_file, text=True)
        
        Stop_code =  result.returncode 
        if Stop_code == 244:
            # stop all the active containers
            print("Stopping all the active containers")
            for cont in client.containers.list():
                try:
                    with open("interrupted.logs","a") as il :
                        print("interrupted container",cont.name, sep="\t",file=il)
                    cont.stop()
                except Exception as e:
                    print(f"Error stopping container {cont.name}: {e}")
            print("All containers stopped due to error code 244.")
            sys.exit(244)

    print(f"Evaluation finished for container {container.name}. Stopping container.")
    try:
        container.stop()
        # container.remove()
    except Exception as e:
        print(f"Error stopping/removing container {container.name}: {e}")

def launch_container( image, ai_proxy_token,external_port, email):
    """
    Launch a container for the given image with AIPROXY_TOKEN set in the environment.
    Returns the container object.
    """
    # Generate a unique container name (using image name and timestamp)
    container_name = f"{email.split('@')[0]}"

    try:
        global token_counter
        token_counter += 1
        try : 
            old_container = client.containers.get(container_name)
            if old_container : 
                if old_container.status == "running":
                    old_container.stop()
                old_container.remove()
        except Exception as e:
            print(f"container {container_name} not found")


        container = client.containers.run(
            image,
            detach=True,
            name=container_name,
            ports={"8000":external_port},
            environment={"AIPROXY_TOKEN": ai_proxy_token},
        )
        print(f"Launched container {container.name} for image {image}")
        return container
    except Exception as e:
        print(f"Failed to launch container for image {image}: {e}")
        return None

def current_total_memory_usage(containers):
    """Sum memory usage of currently running containers in the provided list."""
    total = 0
    for container in containers:
        try:
            container.reload()
        except Exception:
            continue
        if container.status == "running":
            total += get_container_memory_usage(container)
    return total

def main(tsv_file, ai_proxy_token):
    
    active_containers = []     # Containers that are currently running/evaluating
    evaluation_threads = []    # Threads handling evaluation and cleanup
    log_threads = []           # Threads handling log streaming
    
    # Read TSV file; each line is assumed to be: email<TAB>image_name
    images = []
    with open(tsv_file, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            if len(row) >= 2:
                email, image = row[0].strip(), row[1].strip()
                images.append((email, image))
    
    total_images = len(images)
    print(f"Found {total_images} images in the TSV file.")

    for idx, (email, image) in enumerate(images, start=1):
        # Wait until adding a new container does not exceed our RAM limit.
        while True:
            total_usage = current_total_memory_usage(active_containers)
            print(f"Current total container memory usage: {total_usage / (1024**2):.1f} MB")
            if total_usage < TOTAL_RAM_LIMIT and len(active_containers) < MAX_CONTAINER_LIMIT:
                break
            else:
                print("RAM limit reached. Waiting for a container evaluation to finish...")
                time.sleep(CHECK_INTERVAL)
                new_active = []
                new_eval_threads = []
                new_log_threads = []
                for cont, ev_thr, log_thr in zip(active_containers, evaluation_threads, log_threads):
                    try:
                        cont.reload()
                        if cont.status == "running":
                            new_active.append(cont)
                            new_eval_threads.append(ev_thr)
                            new_log_threads.append(log_thr)
                    except Exception:
                        continue
                active_containers = new_active
                evaluation_threads = new_eval_threads
                log_threads = new_log_threads
        external_port = 8000 + idx
        # Launch a new container with the provided AIPROXY_TOKEN.
         
        
        container = launch_container( image, ai_proxy_token,external_port, email=email)
        if container is None:
            continue  # Skip if launch failed

        active_containers.append(container)
        
        # Use the email in the log file name, combined with the unique container name.
        log_file = f"x86_logs/{email}.log"
        # Start a thread to stream container logs to the unique log file.
        log_thread = threading.Thread(target=stream_logs, args=(container, log_file))
        log_thread.start()
        log_threads.append(log_thread)
        
        # Start a thread to poll logs for readiness, evaluate, and then clean up the container.
        eval_thread = threading.Thread(target=evaluate_container, args=(container,email,external_port))
        eval_thread.start()
        evaluation_threads.append(eval_thread)
        
        print(f"Started container {container.name} ({idx}/{total_images}) with log file {log_file}.")
    
    # Wait for all evaluation and log streaming threads to finish.
    for thr in evaluation_threads:
        thr.join()
    for thr in log_threads:
        thr.join()

    print("All container evaluations complete.")

if __name__ == "__main__":
    # Replace 'images.tsv' with the path to your TSV file and provide your AIPROXY_TOKEN.
    main("x86_images.txt", ai_proxy_token="put your ai proxy token")
