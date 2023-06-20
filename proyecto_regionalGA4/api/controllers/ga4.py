from flask import Blueprint, jsonify, request
from api.utils.credentials import validate_credentials, auth_analytics
from api.services.ga4 import list_sites_process_day
from api.utils.send_email import send_email

ads_ga4 = Blueprint('ads_ga4', __name__)

@ads_ga4.route('/ads', methods=['POST'])
def get_advertiser_ga4():
    """Funcion de entrada que procesa los datos del request JSON si es necesario y llama a los services """
    try:
        validate = validate_credentials(request.headers.get('USER_ACCESS'), request.headers.get('PASSWORD'))
        if validate == True:
            auth = auth_analytics("ga4")
            list_sites = list_sites_process_day(auth, request.json['type_extraction'], request.json['init_date'], request.json['end_date'])
            return jsonify({f"Message": f"finish process {list_sites}"}), 200
        else:
            raise Exception("Validation credentials fail...")
    except Exception as e:
        send_email(f'An error occurred while the process was running: GA4 -> {e}')
        return jsonify({'Error': str(e)}), 500