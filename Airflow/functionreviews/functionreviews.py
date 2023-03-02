from nltk.corpus import stopwords
import nltk
import string
nltk.download('stopwords')

def message_cleaning(message):
    '''La función seleciona hace 3 cosas: Poner la cadena en miníscula, eliminar signos de puntuación y quitar stopwords'''
    message = message.lower()
    test_punc_removed = [char for char in message if char not in string.punctuation]
    test_punc_removed_join = "".join(test_punc_removed)
    stop_words = set(stopwords.words('english') + ['cant','im','u','werent','couldnt','dont','didnt','wasnt','us','want','ive','youre','say','said','come','ok','thing','way','put','google','translated','even','see','saw','1st','1','2','3','4','5','6','7','8','9','10'])
    test_punc_removed_join_clean = [palabra for palabra in test_punc_removed_join.split() if palabra not in stop_words]
    join = " ".join(test_punc_removed_join_clean)
    return join