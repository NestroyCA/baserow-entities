curl --request GET \
--url 'https://raw.githubusercontent.com/NestroyCA/working-data/main/json/cleaned_lemma_context.json' \
--header "authorization: token $1" \
-H 'Accept: application/vnd.github.v3.raw' \
-o ./json_dumps/lemma_context.json