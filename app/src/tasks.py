# Third-party packages
from invoke import task


@task
def test(c):
    """Run the test suite."""
    c.run("pytest", pty=False)


if __name__ == "__main__":
    print("Invoke is Awesome!!!!")
