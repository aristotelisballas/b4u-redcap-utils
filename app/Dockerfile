FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .
RUN apt-get -y update
RUN apt-get -y install curl

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the FastAPI application
COPY . .

# Expose the port
EXPOSE 9993

# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9993"]
