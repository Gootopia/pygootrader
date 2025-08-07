import subprocess
from typing import Optional


class DockerHelper:
    @classmethod
    def get_container_env_var(cls, container_name: str, var_name: str) -> Optional[str]:
        """Query a running Docker container for an environment variable value"""
        try:
            result = subprocess.run(
                ["docker", "exec", container_name, "printenv", var_name],
                capture_output=True, text=True, check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            return None
    
    @classmethod
    def is_container_running(cls, container_name: str) -> bool:
        """Check if a container is running"""
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
                capture_output=True, text=True, check=True
            )
            return container_name in result.stdout.strip()
        except subprocess.CalledProcessError:
            return False
        

