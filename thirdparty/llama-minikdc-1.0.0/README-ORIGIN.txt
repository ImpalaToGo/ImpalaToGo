The contents of this directory came from the llama project on 07/18/2014.

They were created by
1) Download from repo
   - Remote is git@github.mtv.cloudera.com:CDH/llama.  This is the Cloudera-
     internal llama repo; there was an external one, but it a the time of this
     work it was out of sync with the internal repo, and broken.
   - On branch "cdh5-1.0.0"
   - The git hash at the time of this work:
     d9066d398cc76b6ebb60f77ccd657d1eb46a667b

2) mvn package -Pdist

3) tar xvfz llama-minikdc-1.0.0-cdh5.2.0-SNAPSHOT.tar.gz in this directory

This project is used for the minikdc it provides.
