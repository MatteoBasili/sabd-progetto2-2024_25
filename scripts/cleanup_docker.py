import subprocess
import os

# Helper to run shell commands
def run(cmd):
    print(f"\n🧹 Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"❌ Command failed: {cmd}")
        exit(1)

def main():
    # Move to project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    os.chdir(project_root)

    # Step 1: Bring down docker-compose environment with volumes and orphans
    run("docker-compose down -v --remove-orphans")

    # Step 2: Remove stopped containers
    run("docker container prune -f")

    # Step 3: Remove unused volumes
    run("docker volume prune -f")

    # Step 4: Remove dangling images
    run("docker image prune -f")

    print("\n✅ Docker environment cleanup complete.")

if __name__ == "__main__":
    main()

