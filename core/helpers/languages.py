from django.db import models
from django.utils.translation import gettext as _

# Standard ISO 639-1 Code


class LanguagesCHO(models.TextChoices):
    ABKHAZIAN = "ab", _("Abkhazian")
    AFAR = "aa", _("Afar")
    AFRIKAANS = "af", _("Afrikaans")
    AKAN = "ak", _("Akan")
    ALBANIAN = "sq", _("Albanian")
    AMHARIC = "am", _("Amharic")
    ARABIC = "ar", _("Arabic")
    ARAGONESE = "an", _("Aragonese")
    ARMENIAN = "hy", _("Armenian")
    AVARIC = "av", _("Avaric")
    AVESTAN = "ae", _("Avestan")
    AYMARA = "ay", _("Aymara")
    AZERBAIJANI = "az", _("Azerbaijani")
    BAMBARA = "bm", _("Bambara")
    BASHKIR = "ba", _("Bashkir")
    BASQUE = "eu", _("Basque")
    BELARUSIAN = "be", _("Belarusian")
    BENGALI = "bn", _("Bengali")
    BIHARI_LANGUAGES = "bh", _("Bihari languages")
    BISLAMA = "bi", _("Bislama")
    BOSNIAN = "bs", _("Bosnian")
    BRETON = "br", _("Breton")
    BULGARIAN = "bg", _("Bulgarian")
    BURMESE = "my", _("Burmese")
    CATALAN = "ca", _("Catalan")
    CENTRAL_KHMER = "km", _("Central Khmer")
    CHAMORRO = "ch", _("Chamorro")
    CHECHEN = "ce", _("Chechen")
    CHINESE = "zh", _("Chinese")
    CHUVASH = "cv", _("Chuvash")
    CORNISH = "kw", _("Cornish")
    CORSICAN = "co", _("Corsican")
    CREE = "cr", _("Cree")
    CROATIAN = "hr", _("Croatian")
    CZECH = "cs", _("Czech")
    DANISH = "da", _("Danish")
    DZONGKHA = "dz", _("Dzongkha")
    ENGLISH = "en", _("English")
    ESPERANTO = "eo", _("Esperanto")
    ESTONIAN = "et", _("Estonian")
    EWE = "ee", _("Ewe")
    FAROESE = "fo", _("Faroese")
    FIJIAN = "fj", _("Fijian")
    FINNISH = "fi", _("Finnish")
    FLEMISH = "nl", _("Dutch; Flemish")
    FRENCH = "fr", _("French")
    FULAH = "ff", _("Fulah")
    GALICIAN = "gl", _("Galician")
    GANDA = "lg", _("Ganda")
    GEORGIAN = "ka", _("Georgian")
    GERMAN = "de", _("German")
    GREENLANDIC = "kl", _("Kalaallisut; Greenlandic")
    GUARANI = "gn", _("Guarani")
    GUJARATI = "gu", _("Gujarati")
    HAITIAN = "ht", _("Haitian; Haitian Creole")
    HAUSA = "ha", _("Hausa")
    HEBREW = "he", _("Hebrew")
    HERERO = "hz", _("Herero")
    HINDI = "hi", _("Hindi")
    HIRI_MOTU = "ho", _("Hiri Motu")
    HUNGARIAN = "hu", _("Hungarian")
    IDO = "io", _("Ido")
    IGBO = "ig", _("Igbo")
    INDONESIAN = "id", _("Indonesian")
    INTERLINGUA = "ia", _("Interlingua")
    INTERLINGUE_OCCIDENTAL = "ie", _("Interlingue; Occidental")
    INUKTITUT = "iu", _("Inuktitut")
    INUPIAQ = "ik", _("Inupiaq")
    IRISH = "ga", _("Irish")
    ITALIAN = "it", _("Italian")
    JAPANESE = "ja", _("Japanese")
    JAVANESE = "jv", _("Javanese")
    KANNADA = "kn", _("Kannada")
    KANURI = "kr", _("Kanuri")
    KASHMIRI = "ks", _("Kashmiri")
    KAZAKH = "kk", _("Kazakh")
    KIKUYU = "ki", _("Kikuyu; Gikuyu")
    KINYARWANDA = "rw", _("Kinyarwanda")
    KOMI = "kv", _("Komi")
    KONGO = "kg", _("Kongo")
    KOREAN = "ko", _("Korean")
    KURDISH = "ku", _("Kurdish")
    KWANYAMA = "kj", _("Kuanyama; Kwanyama")
    KYRGYZ = "ky", _("Kirghiz; Kyrgyz")
    LAO = "lo", _("Lao")
    LATIN = "la", _("Latin")
    LATVIAN = "lv", _("Latvian")
    LIMBURGISH = "li", _("Limburgan; Limburger; Limburgish")
    LINGALA = "ln", _("Lingala")
    LITHUANIAN = "lt", _("Lithuanian")
    LUBA_KATANGA = "lu", _("Luba-Katanga")
    LUXEMBOURGISH = "lb", _("Luxembourgish; Letzeburgesch")
    MACEDONIAN = "mk", _("Macedonian")
    MALAGASY = "mg", _("Malagasy")
    MALAY = "ms", _("Malay")
    MALAYALAM = "ml", _("Malayalam")
    MALDIVIAN = "dv", _("Maldivian")
    MALTESE = "mt", _("Maltese")
    MANX = "gv", _("Manx")
    MAORI = "mi", _("Maori")
    MARATHI = "mr", _("Marathi")
    MARSHALLESE = "mh", _("Marshallese")
    MONGOLIAN = "mn", _("Mongolian")
    NAURU = "na", _("Nauru")
    NAVAHO = "nv", _("Navajo; Navaho")
    NDONGA = "ng", _("Ndonga")
    NEPALI = "ne", _("Nepali")
    NORTHERN_SAMI = "se", _("Northern Sami")
    NORWEGIAN = "no", _("Norwegian")
    NYANJA = "ny", _("Chichewa; Chewa; Nyanja")
    OCCITAN = "oc", _("Occitan (post 1500)")
    OJIBWA = "oj", _("Ojibwa")
    OROMO = "om", _("Oromo")
    OSSETIC = "os", _("Ossetian; Ossetic")
    PALI = "pi", _("Pali")
    PASHTO = "ps", _("Pushto; Pashto")
    PERSIAN = "fa", _("Persian")
    POLISH = "pl", _("Polish")
    PORTUGUESE = "pt", _("Portuguese")
    PUNJABI = "pa", _("Panjabi; Punjabi")
    QUECHUA = "qu", _("Quechua")
    ROMANIAN = "ro", _("Romanian; Moldavian; Moldovan")
    ROMANSH = "rm", _("Romansh")
    RUNDI = "rn", _("Rundi")
    RUSSIAN = "ru", _("Russian")
    SAMOAN = "sm", _("Samoan")
    SANGO = "sg", _("Sango")
    SANSKRIT = "sa", _("Sanskrit")
    SARDINIAN = "sc", _("Sardinian")
    SCOTTISH = "gd", _("Scottish")
    SERBIAN = "sr", _("Serbian")
    SHONA = "sn", _("Shona")
    SICHUAN_YI = "ii", _("Sichuan Yi; Nuosu")
    SINDHI = "sd", _("Sindhi")
    SINHALA = "si", _("Sinhala; Sinhalese")
    SLOVAK = "sk", _("Slovak")
    SLOVENIAN = "sl", _("Slovenian")
    SOMALI = "so", _("Somali")
    SPANISH = "es", _("Spanish")
    SUNDANESE = "su", _("Sundanese")
    SWAHILI = "sw", _("Swahili")
    SWATI = "ss", _("Swati")
    SWEDISH = "sv", _("Swedish")
    TAGALOG = "tl", _("Tagalog")
    TAHITIAN = "ty", _("Tahitian")
    TAJIK = "tg", _("Tajik")
    TAMIL = "ta", _("Tamil")
    TATAR = "tt", _("Tatar")
    TELUGU = "te", _("Telugu")
    THAI = "th", _("Thai")
    TIBETAN = "bo", _("Tibetan")
    TIGRINYA = "ti", _("Tigrinya")
    TONGA = "to", _("Tonga (Tonga Islands)")
    TSONGA = "ts", _("Tsonga")
    TSWANA = "tn", _("Tswana")
    TURKISH = "tr", _("Turkish")
    TURKMEN = "tk", _("Turkmen")
    TWI = "tw", _("Twi")
    UKRAINIAN = "uk", _("Ukrainian")
    URDU = "ur", _("Urdu")
    UYGHUR = "ug", _("Uighur; Uyghur")
    UZBEK = "uz", _("Uzbek")
    VENDA = "ve", _("Venda")
    VIETNAMESE = "vi", _("Vietnamese")
    VOLAPÜK = "vo", _("Volapük")
    WALLOON = "wa", _("Walloon")
    WELSH = "cy", _("Welsh")
    WOLOF = "wo", _("Wolof")
    XHOSA = "xh", _("Xhosa")
    YIDDISH = "yi", _("Yiddish")
    YORUBA = "yo", _("Yoruba")
    ZHUANG = "za", _("Zhuang; Chuang")
    ZULU = "zu", _("Zulu")