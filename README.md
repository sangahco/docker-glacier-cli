# AWS Glacier CLI with Multipart Upload


## Requirements

First make sure Docker and Docker Compose are installed on the machine with:

    $ docker -v
    $ docker-compose -v

If they are missing, follow the instructions on the official website (they are not hard really...):

- [Docker CE Install How-to](https://docs.docker.com/engine/installation/)
- [Docker Compose Install How-to](https://docs.docker.com/compose/install/)


## How to use this images

First of all you need to set some environment variables in order to use this script:

- **AWS_ACCESS_KEY_ID**
  
  The AWS access key
  
- **AWS_SECRET_ACCESS_KEY**

  The AWS secret key
  
- **AWS_DEFAULT_REGION**  

  The AWS default region
  
- **AWS_VAULT**  
  
  The AWS Vault name


**Use the script `docker-auto.sh` to manage these services!**

    $ ./docker-auto.sh --help
    
Backup a file with the following command:

    $ GLACIER_DATA=./data ./docker-auto.sh run -f filename_here -m 'some description here'
    
make sure to set the environment variable **GLACIER_DATA** to the folder where the file is located.

Delete an archive from the Glacier Vault with the following command:

    $ ./docker-auto.sh run -d <archive id>

Retrieve the archive list:

    $ ./docker-auto.sh run --archive-list

Retrieve the job list:

    $ ./docker-auto.sh run --jobs

Retrieve the output of a job (json or binary depending on the job type):

    $ ./docker-auto.sh run --job <job id>