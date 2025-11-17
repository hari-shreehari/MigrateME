# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install Conda
RUN apt-get update && apt-get install -y wget &&     wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh &&     bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/conda &&     rm Miniconda3-latest-Linux-x86_64.sh

# Add Conda to PATH
ENV PATH /opt/conda/bin:$PATH

# Create the Conda environment
COPY backend_env.yml .
RUN conda env create -f backend_env.yml

# Activate the Conda environment
SHELL ["conda", "run", "-n", "migrateme", "/bin/bash", "-c"]

# Make port 80 available to the world outside this container
EXPOSE 80

# Run the application
CMD ["uvicorn", "BackEnd.main:app", "--host", "0.0.0.0", "--port", "80"]
