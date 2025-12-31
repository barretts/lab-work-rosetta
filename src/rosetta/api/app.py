"""Flask application factory and server runner."""

import os
from typing import Optional

from flask import Flask
from flask_cors import CORS

from rosetta.api.routes import register_routes


def create_app(db_path: Optional[str] = None) -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__)

    # Configuration
    app.config["JSON_SORT_KEYS"] = False
    app.config["ROSETTA_DB_PATH"] = db_path or os.getenv("ROSETTA_DB_PATH")

    # Enable CORS for all routes
    CORS(app)

    # Register routes
    register_routes(app)

    return app


def run_server(host: str = "0.0.0.0", port: int = 5000, debug: bool = False) -> None:
    """Run the Flask development server."""
    app = create_app()
    print(f"\n🔬 Clinical Rosetta Stone API")
    print(f"   Running on http://{host}:{port}")
    print(f"   Debug mode: {debug}")
    print(f"\nEndpoints:")
    print(f"   GET  /                     - API info")
    print(f"   GET  /health               - Health check")
    print(f"   GET  /stats                - Database statistics")
    print(f"   GET  /translate?q=<name>   - Translate test name to LOINC")
    print(f"   POST /translate/batch      - Batch translate test names")
    print(f"   GET  /loinc/<code>         - Get LOINC code details")
    print(f"   GET  /search?q=<query>     - Search for lab tests")
    print(f"   GET  /reference-range/<code> - Get reference ranges")
    print(f"   GET  /critical-values      - Get critical values")
    print(f"   GET  /drugs                - Get drug-lab interactions")
    print()
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    run_server(debug=True)
