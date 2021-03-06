# Rake

## Objective
To gather metagenomics sequencing file paths from a variety of locations on S3, and fill in any missing data
by dispatching the jobs in AWS batch. The name is because tasks like this make me feel like an S3 gardener.

## Context
To carry out a contrast analysis on 190 libraries of metagenomic samples, we want to collect some pertinent 
information generated during past analyses that are kept on AWS S3. 
These includes FastQC reports, contigs assembled by SPAdes, genes that were called by Prodigal, and more. 
Unfortunately these files are sometimes (often) mis-labeled or missing, and need to be re-generated.

This repository shows part of my solution to this problem using AWS services.

## Workflow
### S3_crawler.py
Find a list of files on S3 for each library, by matching it to a specific pattern outlined in the `target_paths` 
dictionnary. If no files match the patterns, the path is marked as `NA`.

### batch.py
Use the list of paths generated above to dispatch jobs. If a path is `NA`, it needs to be generated by a batch job.
Using the arguments in `batch.yml`, generate the batch job to create the missing file.

### docker image build
The image is built using the `Dockerfile` in the docker folder:
`docker build -t good-science .`

We then tag and push the image to ECR, the AWS docker repository:

`docker tag good-science my-account.dkr.ecr.us-west-2.amazonaws.com/good-science
docker push my-account.dkr.ecr.us-west-2.amazonaws.com/good-science`

After the batch jobs are done, we can re-run S3_crawler to find all remaining paths.

### Generate the master data frame
Using the paths on S3, get the data and collate it into a master df. And voila! We finally have all the data in one 
place, and can carry out the analysis.

