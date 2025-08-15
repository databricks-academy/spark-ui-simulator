import os, sys
sys.path.append("C:\dvlp\databricks-academy\spark-ui-simulator-app\spark-ui-simulator")

from functions import Capture

capture = Capture(working_dir=os.getcwd(),
                  notebook_path="/Repos/student@azuredatabrickstraining.onmicrosoft.com/spark-ui-simulator-experiments/Exp #5136 - Cluster-Attached Drives/Exp #5136A - Cluster-Attached Drives, HDD",
                  headless=True,
                  version="v003-P",
                  languages=["python"],
                  has_lab=True)

capture.download_all()

