# Use the official Python image as the base image
FROM python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy the source code and data into the container
COPY src/ /app/src/
COPY data/ /app/data/
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install -r requirements.txt

# Run the main.py script
CMD ["python", "src/main.py"]