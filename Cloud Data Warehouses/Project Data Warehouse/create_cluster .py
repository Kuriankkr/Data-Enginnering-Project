import pandas as pd
import boto3
import json
import psycopg2

from botocore.exceptions import ClientError
import configparser


def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    '''
    Creates IAM Role for Redshift, to allow it to use AWS services
    Note: Have to create IAM user first and get details for that. IAM role doesnt have right to access AWS services unless delegated by IAM User
    At this point you can get the IAM role ARN
    
    Args:
    iam: Argument for the iam boto3 client
    DWH_IAM_ROLE_NAME: Argument for the Rolename
    
    
    Returns:
    The newly created IAM role
    
    Raises:
    Entity Already Exists: If the IAM role has alrady been creted the error will be raised
    '''
    
    

    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
        
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    return roleArn


def create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):
    '''
    Creates Redshift cluster
    
    Args: 
    redshift: Argument for the redshift boto3 client
    roleArn: Argument for the IAM role ARN
    DWH_CLUSTER_TYPE: Argument for the type of cluster to be created
    DWH_NODE_TYPE: Arguemnt for the type each node in the cluster
    DWH_NUM_NODES: Argument for the number of nodes
    DWH_DB: Argument for the Database name
    DWH_CLUSTER_IDENTIFIER:Argument for the indentifying the cluster
    DWH_DB_USER: Argument for the database user name
    DWH_DB_PASSWORD: Argument for the database entry password
    
    Returns:
    This function doesnt return anything 
    
    Raises:
    Entity already exists: Cluster alrady exists
    
    '''

    try:
        response = redshift.create_cluster(        
        
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)


def get_cluster_details(redshift, DWH_CLUSTER_IDENTIFIER):
    '''
    Retrieve Redshift clusters properties..(similar to the dataframe printed in the other excesrcise with all the details)
    
    Arguments:
    redshift: Argument for the redshift boto3 client
    DWH_CLUSTER_IDENTIFIER: Argument for the indentifying the cluster
    
    Returns:
    myClusterProps: Dictionary containing the details of the newly created redshift cluster in a key-value fashion
    DWH_ENDPOINT: The endpoint of the newly created cluster inorder to connect to it
    DWH_ROLE_ARN: The arn detail of the cluster
    '''

    def prettyRedshiftProps(props):
        
        '''
        Arguments:
        props: The dictionary with the newly created cluster key-value combinations
        
        Returns:
        Dataframe with key-value columns
        
        '''
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    
    '''
    Get the details oof the cluster and put them into dwh.cfg file for further process
    '''
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    return myClusterProps, DWH_ENDPOINT, DWH_ROLE_ARN


def open_ports(ec2, myClusterProps, DWH_PORT):
    '''
    Update clusters security group to allow access through redshift port. 
    
    Arguments:
    ec2: Argument for the ec2 boto3 client
    myClusterProps: Argument for the dictionary containing the details of the newly created redshift cluster in a key-value fashion
    DWH_PORT: Argument with the connecting PORT number
    
    Return:
    This function does not return any value
    
    Raises:
    Entity already exists: Port alrady exists
    
    '''

    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)


def main():
    
    '''
    The main function that calls all the other functions from within
    '''
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    df = pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                })

    print(df)


    ec2 = boto3.resource('ec2',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                    )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)

    create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)

    myClusterProps = get_cluster_details(redshift, DWH_CLUSTER_IDENTIFIER)

    open_ports(ec2, myClusterProps, DWH_PORT)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()    
    print('Connected')

    conn.close()


if __name__ == "__main__":
    main()
