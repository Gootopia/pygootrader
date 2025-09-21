import subprocess
import os
from typing import Optional


class DockerHelper:
    @classmethod
    def get_container_env_var(cls, 
                              container_name: str, 
                              var_name: str) -> Optional[str]:
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
    def is_container_running(cls, 
                             container_name: str) -> bool:
        """Check if a container is running"""
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
                capture_output=True, text=True, check=True
            )
            return container_name in result.stdout.strip()
        except subprocess.CalledProcessError:
            return False
    
    @classmethod
    def start_service(cls, 
                      service_name: str = None,
                      is_detached: bool = True, 
                      use_local_compose_file: bool = True,
                      compose_file_path: str = "docker-compose.yml"):
        """Start a service container"""
        assert service_name is not None, "service_name parameter is required"
        
        if use_local_compose_file:
            assert os.path.exists(compose_file_path), f"Compose file '{compose_file_path}' not found"
            cmd = ["docker-compose", "-f", compose_file_path, "up"]
            if is_detached:
                cmd.append("-d")
            cmd.append(service_name)
            subprocess.run(cmd, check=True)

