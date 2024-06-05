from diagrams import Diagram, Cluster
from diagrams.aws.compute import EC2
from diagrams.aws.database import RDS
from diagrams.aws.network import ELB

with Diagram("Data pipeline", show=False):
    with Cluster("EV data"):
        ELB("EV url") >> EC2("collection")  >> EC2("profiling") >> EC2("cleaning") >> RDS("storage: ev-infra.sqlite")

    with Cluster("Fuel data"):
         ELB("fuel url") >> EC2("collection")  >> EC2("profiling") >> EC2("cleaning") >> RDS("storage: fuel-prices.sqlite")
    
   