from flask import Blueprint, jsonify, request
from api.services.universal import list_sites_process_day
from api.utils.credentials import auth_analytics, validate_credentials
from api.utils.send_email import send_email
from api.utils.mange_enviroment_varible import get_variable

ads_universal = Blueprint('ads_universal', __name__)

@ads_universal.route('/ads', methods=['POST'])
def get_advertiser():
    """Funcion de entrada que procesa los datos del request JSON si es necesario y llama a los services """
    try:
        validate = validate_credentials(request.headers.get('USER_ACCESS'), request.headers.get('PASSWORD'))
        if validate == True:
            auth = auth_analytics("universal")
            list_sites = list_sites_process_day(auth,request.json['type_extraction'], request.json['init_date'], request.json['end_date'])
            return jsonify({"Message": list_sites}), 200
        else:
            raise Exception("Validation credentials fail...")
    except Exception as e:
        send_email(f'An error occurred while the process was running: Universal -> {e}')
        return jsonify({'Error': str(e)}), 500

