# DEBS'25 Grand Challenge local challenger

This is the local evaluation platform for the DEBS 2025 Grand Challenge.

The challenger is executed as a Docker container that binds to a local port.

You can start benchmark runs, receive input batches, submit results and record
the timings of the test. Additionally you can consult your previous runs and
generate simple visualizations to check the correctness of your solution.

## Running

1. Extract the two archives
    - `gc25-chall.tgz`: contains all the required files excluding the database
    - `gc25-chall-data.tgz`: contains the database with the inputs

    > Note: this guide expects the two archives to be extracted in the same directory

2. Load and run the container
    - See the `run.sh` script for the commands required to load the image and run it

## Usage

The docker file contains a web server with a REST interface.
In the archive you will find both an example unoptimized python solution and an openapi.yml specification.

The example client `client_ref.py` shows an example of how to use the local challenger, the `openapi.yml` contains the OpenAPI3/Swagger specification for the REST API, you can use it as reference to develop your solution. Note however that at the current time the API is not stable yet, we do not expect large changes, but this should be kept into consideration.

Finally, the webserver has a `/dash` http endpoint that can be used to browse the results of the benchmarking runs and plot the submitted results.
> Note that you must send the `/api/end/{bench_id}` POST request for results to be available in the dash

The `run.sh` script is configured to bind local port 8866. Example: `http://127.0.0.1:8866/api/create`

---

# Contact

Be sure to send us an email if you encounter problems.