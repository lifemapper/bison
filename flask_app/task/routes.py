"""URL Routes for the Specify Network API services."""
from flask import Blueprint, Flask, render_template

from flask_app.common.constants import (STATIC_DIR, TEMPLATE_DIR)

bison_blueprint = Blueprint(
    "task", __name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR,
    static_url_path="/static")

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.register_blueprint(bison_blueprint)


# .....................................................................................
@app.route('/')
def index():
    """Render template for the base URL.

    Returns:
        Rendered template for a browser response.
    """
    return render_template("task.index.html")

# .....................................................................................
# .....................................................................................
if __name__ == "__main__":
    app.run(debug=True)
