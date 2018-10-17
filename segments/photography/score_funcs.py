import json
import math

from ContentMetadataS3Retriever import ContentMetadataS3Retriever


WORD_COUNT_PER_MINUTE = 300
WORD_COUNT_PER_SECOND = WORD_COUNT_PER_MINUTE / 60


contentMetadataS3Retriever = ContentMetadataS3Retriever()
CONTENT_WORD_COUNT = contentMetadataS3Retriever.get_content_word_count()


def eq(s1, s2):
    return s1.lower() == s2.lower()


def scroll_depth_to_score(scroll_depth):
    scroll_depth_int = int(float(scroll_depth))
    return {
        25: 2.5,
        50: 5.0,
        75: 7.5,
        100: 10.0
    }.get(scroll_depth_int, 0.0)


def time_on_page_to_score(time_on_page, word_count):
    completion_ratio = time_on_page * WORD_COUNT_PER_SECOND / (word_count * 1.0)
    if completion_ratio < 0.1:
        return 0
    rounded_score = int(math.ceil(completion_ratio * 10))
    return min(rounded_score, 10)


def feedback_value_to_score(feedback_value):
    if feedback_value:
        return 10
    return -15


def interest_to_score(interest_list):
    if 'photography' in interest_list:
        return 50
    return -75


def score_single_event(event_name, properties_str):
    properties = json.loads(properties_str)
    score = 0

    if eq(event_name, 'ContentItem_Received'):
        score = 1

    if eq(event_name, 'ContentItem_View'):
        score = 1

    elif eq(event_name, 'ContentItem_PageScroll'):
        score = scroll_depth_to_score(properties.get('ScrollDepth'.lower(), 0))

    elif eq(event_name, 'ContentItem_TimeOnPage'):
        time_on_page = properties.get('TimeOnPage'.lower(), -1.0)
        word_count = CONTENT_WORD_COUNT.get(properties['MessageType'.lower()].lower(), -1)
        if (word_count > 0) and (time_on_page > 0):
            score = time_on_page_to_score(time_on_page, word_count)

    elif (eq(event_name, 'ContentItem_Click')
          and properties.get('ActionId'.lower()) in
            ['FeedbackModuleWasUseful'.lower(), 'FeedbackModule'.lower()]
          and properties.get('FeedbackValue'.lower()) is not None):
        score = feedback_value_to_score(properties['FeedbackValue'.lower()])

    elif (eq(event_name, 'ContentItem_Click')
          and 'ActionId'.lower() in properties
          and properties['ActionId'.lower()] not in
            [x.lower() for x in ['FeedbackModuleWasUseful', 'FeedbackModule', 'CarouselSwipe',
                                 'BackButton']]):
        score = 1

    elif eq(event_name, 'Timeline_OnboardingMessage_Click'):
        score = interest_to_score(properties.get('SelectedItems'.lower(), []))

    return float(score)
