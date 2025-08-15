import os, sys
sys.path.append("C:\dvlp\databricks-academy\spark-ui-simulator-app\spark-ui-simulator")

from functions import Capture

capture = Capture(working_dir=os.getcwd(),
                  notebook_path="/Repos/student@azuredatabrickstraining.onmicrosoft.com/spark-ui-simulator-experiments/Exp #9210 - Wide Tables/Exp #9210A-P - Wide Tables without Photon",
                  headless=True,
                  version="v003-P",
                  languages=["python"],
                  has_lab=False)

capture.download_all()

