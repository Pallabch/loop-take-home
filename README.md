# loop-take-home Project
This is a simple implementation of an api using FastAPI. The data is fetched from a free tier mongodb server whose credentials are stored in an env file. 

The utilities folder contains the logic for computation of the uptime and downtime of the restaurants as well as the code for accessing the mongodb database. 

The trigger and polling logic has been implemented as described in the notion document.

To run the project you'd need an env file which I can pass on through a secure channel.

First Install modules from requirements.txt 

Command to Run the API:

uvicorn main:app --reload