from sp_api.api.messaging.messaging import Messaging
from sp_api.base import Marketplaces


def test_create_negative_feedback_removal_request():
    res = Messaging().create_negative_feedback_removal('123-1234567-1234567')
    print(res)
