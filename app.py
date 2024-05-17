from flask import Flask
from scheduler import classScheduler

app = Flask(__name__)

@app.route("/")
def hello_world():
    options = {"seconds": 300}
    scheduler = classScheduler()
    #scheduler.solve(solver_name="cbc", solver_path=cbc_path, options=options)
    scheduler.solve(solver_name="bonmin", local=False, options=None)
    #scheduler.solve(model,opt="bonmin")
    return "OK"