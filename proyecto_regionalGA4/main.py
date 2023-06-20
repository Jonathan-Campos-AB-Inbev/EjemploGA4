import os
from flask import Flask
from flask_cors import CORS
from api.controllers.universal import ads_universal
from api.controllers.ga4 import ads_ga4


json_parameter = None
app = Flask(__name__)
CORS(app)

app.register_blueprint(ads_universal, url_prefix="/google_ads_universal")
app.register_blueprint(ads_ga4, url_prefix="/google_ads_ga4")

if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))