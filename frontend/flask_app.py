#!/home/ubuntu/miniconda3/envs/snakes/bin/python
from flask import Flask, render_template, request, redirect, url_for, jsonify

# initialize flask
app = Flask(__name__)

@app.route('/')

def main():
	return render_template("tab.html")

if __name__ == "__main__":
	# update running app
	app.config['DEBUG'] = True
	# point from domain name to ec2
	app.run(host="0.0.0.0", port=80)

#which python
# Run by : screen sudo /home/ubuntu/miniconda3/envs/snakes/bin/python fe.py