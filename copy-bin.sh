#!/bin/zsh
# Run inside the project directory of holi-search-engine

pem=holi.pem

node1=34.235.126.115
node2=54.173.64.210
node3=3.84.206.138
node4=54.90.122.167

scp -i ${pem} -r bin ec2-user@${node1}:
scp -i ${pem} -r bin ec2-user@${node2}:
scp -i ${pem} -r bin ec2-user@${node3}:
scp -i ${pem} -r bin ec2-user@${node4}:

echo ssh -i ${pem} ec2-user@${node1}
echo ssh -i ${pem} ec2-user@${node2}
echo ssh -i ${pem} ec2-user@${node3}
echo ssh -i ${pem} ec2-user@${node4}