FROM aws-account.dkr.ecr.us-west-2.amazonaws.com/base-container

# dependencies
COPY docker_requirements.txt .
RUN pip install --upgrade pip && pip install -r docker_requirements.txt

# install SPADEs
RUN wget http://cab.spbu.ru/files/release3.11.1/SPAdes-3.11.1-Linux.tar.gz
RUN tar xzf SPAdes-3.11.1-Linux.tar.gz
RUN rm SPAdes-3.11.1-Linux.tar.gz && mv SPAdes-3.11.1-Linux/ spades/

# Add scripts
COPY * ./
WORKDIR /
