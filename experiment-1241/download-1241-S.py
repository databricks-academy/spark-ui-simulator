from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
                  notebook_path="/Curriculum/Spark-UI-Simulator/Exp #1241-S - maxPartitionBytes",
                  headless=True,
                  version="v002-S",
                  languages=["scala"],
                  has_lab=False)

# capture.download_all()
capture.sign_in()
capture.notebook()
capture.finish()

