from watson_developer_cloud import AlchemyLanguageV1

alchemy_language = AlchemyLanguageV1(api_key='ebdea24bbf82e371fa9c25a6deb59b85b65c0ede')


def get_concepts_from_watson(text):
    concepts = {}
    data = alchemy_language.combined(
        text=text)['concepts']
    for c in data:
        category_name = c['text']
        relevance = float(c['relevance'])
        concepts[category_name] = relevance
    return concepts
