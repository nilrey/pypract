import docker

def run_php_test_container():
    try:
        # Создаем клиент Docker
        client = docker.DockerClient(base_url='unix:///home/sadmin/.docker/desktop/docker.sock')  # Укажите ваш путь

        print("Available images:")
        images = client.images.list()
        for img in images:
            print(img.tags)

        print("Checking for 'center-php' image locally...")
        # Проверяем, есть ли образ локально
        if not any("center-php" in tag for image in images for tag in image.tags):
            raise ValueError("The 'center-php' image is not available locally. Please build or load the image first.")

        print("Running the container...")
        # Запускаем контейнер в режиме detach
        container = client.containers.run("center-php", detach=True, name="php_test_container")

        print("Container is running. Use 'docker ps' to see it.")

    except ValueError as ve:
        print(f"Error: {ve}")
    except docker.errors.DockerException as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    run_php_test_container()
