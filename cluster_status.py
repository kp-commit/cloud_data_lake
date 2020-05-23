#!/opt/conda/bin/python
"""
cluster_status.py: Check EMR Cluster Status
- list_all_clusters
- describe_cluster
- fmt_description
- get_status
- display_all_cluster_status
"""
import boto3
import config as c


# Def - get list of active EMR clusters
def list_all_clusters(emr_client):
    clusters = emr_client.list_clusters(ClusterStates=['STARTING', \
                                                       'BOOTSTRAPPING', \
                                                       'RUNNING', 'WAITING'])
    cluster_ids = [cltr["Id"] for cltr in clusters["Clusters"]]
    return cluster_ids

# Def - get descriptions of EMR clusters
def describe_cluster(emr_client, cluster_id):
    description = emr_client.describe_cluster(ClusterId=cluster_id)
    state = description['Cluster']['Status']['State']
    name = description['Cluster']['Name']
    cid = description['Cluster']['Id']
    description = {'id': cid, 'state': state, 'name': name}
    return description

# Def - format description 
def fmt_description(description):
    ln = "CLUSTER_ID: {id}  NAME: '{name}'  STATE: {state}" \
    .format(id=description['id'], name=description['name'], state=description['state'])
    return ln

# Def - get status for cluster_id
def get_status(emr_client, cluster_id):
    description = describe_cluster(emr_client, cluster_id)
    print(fmt_description(description))
     
def display_all_cluster_status(cids):
    for cid in cids:
        get_status(emr, cid)

# EMR client
emr = boto3.client('emr',
                     region_name=c.REGION_NAME,
                     aws_access_key_id=c.AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=c.AWS_SECRET_ACCESS_KEY
)

# Print available clusters status
all_clusters = list_all_clusters(emr)

print('ACTIVE CLUSTERS in {}:'.format(c.REGION_NAME.upper()))
display_all_cluster_status(all_clusters)

