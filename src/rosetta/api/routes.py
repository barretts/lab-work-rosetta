"""API route definitions."""

from flask import Flask, jsonify, request

from rosetta import __version__
from rosetta.core.resolver import LabTestResolver


def register_routes(app: Flask) -> None:
    """Register all API routes."""

    @app.route("/")
    def index():
        """API information endpoint."""
        return jsonify(
            {
                "name": "Clinical Rosetta Stone API",
                "version": __version__,
                "description": "Lab test translation and standardization service",
                "endpoints": {
                    "/": "API info",
                    "/health": "Health check",
                    "/stats": "Database statistics",
                    "/translate": "Translate test name to LOINC (GET ?q=name)",
                    "/translate/batch": "Batch translate (POST with JSON array)",
                    "/loinc/<code>": "Get LOINC code details",
                    "/search": "Search for lab tests (GET ?q=query)",
                    "/reference-range/<code>": "Get reference ranges",
                    "/critical-values": "Get critical values",
                    "/drugs": "Get drug-lab interactions",
                },
            }
        )

    @app.route("/health")
    def health():
        """Health check endpoint."""
        try:
            with LabTestResolver() as resolver:
                stats = resolver.get_stats()
                return jsonify(
                    {
                        "status": "healthy",
                        "database": "connected",
                        "loinc_codes": stats.get("LOINC codes", 0),
                    }
                )
        except Exception as e:
            return jsonify({"status": "unhealthy", "error": str(e)}), 500

    @app.route("/stats")
    def stats():
        """Database statistics endpoint."""
        try:
            with LabTestResolver() as resolver:
                return jsonify({"status": "ok", "stats": resolver.get_stats()})
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.route("/translate")
    def translate():
        """
        Translate a lab test name to LOINC code.

        Query params:
            q: Test name to translate (required)
            confidence: Minimum confidence threshold (default: 0.5)
        """
        query = request.args.get("q", "").strip()
        if not query:
            return jsonify({"error": "Missing required parameter: q"}), 400

        min_confidence = float(request.args.get("confidence", 0.5))

        try:
            with LabTestResolver() as resolver:
                result = resolver.resolve(query, min_confidence=min_confidence)

                if result:
                    # Get additional details
                    details = resolver.get_loinc_details(result["loinc_code"])
                    if details:
                        result["details"] = {
                            "component": details.get("component"),
                            "system": details.get("system"),
                            "description": details.get("description"),
                            "example_units": details.get("example_units"),
                        }
                        if details.get("reference_ranges"):
                            result["reference_ranges"] = details["reference_ranges"]
                        if details.get("critical_values"):
                            result["critical_values"] = details["critical_values"]

                    return jsonify({"status": "ok", "input": query, "result": result})
                else:
                    return jsonify({"status": "not_found", "input": query, "result": None})
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.route("/translate/batch", methods=["POST"])
    def translate_batch():
        """
        Batch translate lab test names.

        Request body: JSON array of test names
        Returns: Array of translation results
        """
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 400

        data = request.get_json()
        if not isinstance(data, list):
            return jsonify({"error": "Request body must be a JSON array"}), 400

        min_confidence = float(request.args.get("confidence", 0.5))
        results = []

        try:
            with LabTestResolver() as resolver:
                for test_name in data:
                    if not isinstance(test_name, str):
                        results.append({"input": test_name, "error": "Invalid input"})
                        continue

                    result = resolver.resolve(test_name, min_confidence=min_confidence)
                    results.append(
                        {
                            "input": test_name,
                            "loinc_code": result["loinc_code"] if result else None,
                            "loinc_name": result["loinc_name"] if result else None,
                            "confidence": result["confidence"] if result else None,
                            "method": result["method"] if result else None,
                        }
                    )

            return jsonify({"status": "ok", "count": len(results), "results": results})
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.route("/loinc/<code>")
    def loinc_details(code: str):
        """Get full details for a LOINC code."""
        try:
            with LabTestResolver() as resolver:
                details = resolver.get_loinc_details(code)

                if details:
                    return jsonify({"status": "ok", "loinc_code": code, "details": details})
                else:
                    return (
                        jsonify({"status": "not_found", "loinc_code": code, "details": None}),
                        404,
                    )
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.route("/search")
    def search():
        """
        Search for lab tests by name.

        Query params:
            q: Search query (required)
            limit: Maximum results (default: 20)
        """
        query = request.args.get("q", "").strip()
        if not query:
            return jsonify({"error": "Missing required parameter: q"}), 400

        limit = min(int(request.args.get("limit", 20)), 100)

        try:
            with LabTestResolver() as resolver:
                results = resolver.search(query, limit=limit)
                return jsonify(
                    {"status": "ok", "query": query, "count": len(results), "results": results}
                )
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.route("/reference-range/<code>")
    def reference_range(code: str):
        """
        Get reference ranges for a LOINC code.

        Query params:
            age: Patient age (optional)
            sex: Patient sex - M/F (optional)
        """
        age = request.args.get("age", type=int)
        sex = request.args.get("sex")

        try:
            with LabTestResolver() as resolver:
                result = resolver.get_reference_range(code, age=age, sex=sex)

                if result:
                    return jsonify({"status": "ok", "loinc_code": code, "range": result})
                else:
                    return jsonify({"status": "not_found", "loinc_code": code, "range": None})
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.route("/critical-values")
    def critical_values():
        """
        Get critical values.

        Query params:
            loinc: LOINC code (optional, returns all if not specified)
        """
        loinc_code = request.args.get("loinc")

        try:
            with LabTestResolver() as resolver:
                results = resolver.get_critical_values(loinc_code)
                return jsonify({"status": "ok", "count": len(results), "values": results})
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.route("/drugs")
    def drugs():
        """
        Get drug-lab interactions.

        Query params:
            name: Drug name filter (optional)
        """
        drug_name = request.args.get("name")

        try:
            with LabTestResolver() as resolver:
                results = resolver.get_drug_interactions(drug_name)
                return jsonify({"status": "ok", "count": len(results), "drugs": results})
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"error": "Endpoint not found"}), 404

    @app.errorhandler(500)
    def server_error(e):
        return jsonify({"error": "Internal server error"}), 500
