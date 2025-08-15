from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
                  notebook_path="/Curriculum/Spark-UI-Simulator/Exp #0000-P - Exploring the Spark UI",
                  headless=True,
                  version="v002-P",
                  languages=["python", "scala"],
                  has_lab=True)

capture.all()

# capture.sign_in()
# capture.cluster_libraries()
# capture.finish()
#
#
