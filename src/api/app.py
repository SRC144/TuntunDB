from flask import Flask
from flask_cors import CORS
from .routes import api_bp
from .utils import CustomJSONEncoder

def create_app(test_config=None):
    """Create and configure the Flask application"""
    app = Flask(__name__)
    
    # Enable CORS
    CORS(app, resources={
        r"/api/*": {
            "origins": ["http://localhost:3000"],
            "methods": ["GET", "POST", "OPTIONS"],
            "allow_headers": ["Content-Type"]
        }
    })
    
    # Configure JSON encoding
    app.json_encoder = CustomJSONEncoder
    
    # Default configuration
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE_PATH='src/data'  # Path to database files
    )

    if test_config is not None:
        # Load test config if passed in
        app.config.update(test_config)
        
    # Register blueprints
    app.register_blueprint(api_bp, url_prefix='/api')
    
    return app 