from flask import Flask, send_from_directory, Response, render_template, redirect, jsonify, request
from dotenv import load_dotenv
import os
from justframeit import justframeit_bp

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Register blueprints
app.register_blueprint(justframeit_bp)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/justframeit')
def justframeit():
    return render_template('justframeit.html')

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=8000)
    
#small change to test deployment
