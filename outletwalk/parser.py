import json


def inspect(payload):
    document = json.loads(payload)
    if not isinstance(document, dict) or not isinstance(document.get('value'), list):
        raise ValueError('Expected collection value array')
    if any(not isinstance(item, dict) for item in document['value']):
        raise ValueError('Collection records must be objects')
    return {'records': document['value'], 'next': document.get('@odata.nextLink')}
