from enum import Enum
from pydantic import BaseModel
from open_dictionary.llm.llm_client import get_chat_response


class Category(str, Enum):
    GENERAL = "General"
    BIOLOGY = "Biology"
    CHEMISTRY = "Chemistry"
    PHYSICS = "Physics"
    MATHEMATICS = "Mathematics"
    TECHNOLOGY_COMPUTER_SCIENCE = "Technology / Computer Science"
    MEDICINE = "Medicine"
    SOCIAL_SCIENCES = "Social Sciences"
    HUMANITIES = "Humanities"
    BUSINESS_ECONOMICS = "Business & Economics"
    LAW = "Law"
    ARTS_CULTURE = "Arts & Culture"
    ARCHAIC_OBSOLETE = "Archaic / Obsolete"


class Regularity(BaseModel):
    word: str
    is_common: bool
    reasoning: str
    category: Category

instruction = """
You are an expert linguist and lexicographer. Your task is to evaluate an English word or phrase based on its commonality and its subject category, responding with a single JSON object.

**1. Commonality Check:**
Determine if the word/phrase is 'common'. A 'common' word meets a very high bar: it is used in the vast majority (e.g., 99.9%) of everyday life, conversations, and the standard K-12 school curriculum. It should be instantly recognizable and understood by nearly all native English speakers, regardless of their profession or education level. If a word is primarily known through specific high school subjects (like AP Biology) but not used outside of that context, it is not 'common'.

**2. Category Classification:**
Classify the word/phrase into **one** of the following fixed categories. You **must** select the best fit from this list only.

*   `General`: For words used broadly across all contexts.
*   `Biology`: Concepts related to living organisms.
*   `Chemistry`: Concepts related to substances, their properties, and reactions.
*   `Physics`: Concepts related to matter, energy, and their interactions.
*   `Mathematics`: Concepts related to numbers, quantity, and space.
*   `Technology / Computer Science`: Terms related to applied science, computing, and the internet.
*   `Medicine`: Terms specific to health, disease, and clinical practice.
*   `Social Sciences`: Terms from sociology, psychology, anthropology, and political science.
*   `Humanities`: Terms from philosophy, literature, and history.
*   `Business & Economics`: Terms related to commerce, finance, and the economy.
*   `Law`: Terms specific to the legal profession and system.
*   `Arts & Culture`: Terms related to music, visual arts, and cultural practices.
*   `Archaic / Obsolete`: Words that are no longer in common use.

**3. Output Format:**
Your response **must** be a single, valid JSON object with the following keys:
*   `"word"`: The input word or phrase being evaluated.
*   `"is_common"`: A boolean (`true` or `false`).
*   `"reasoning"`: A brief explanation for the commonality classification, justifying your choice.
*   `"category"`: The classified subject category, chosen from the fixed list above.

---

**[Example 1]**

**Input:** water

**Output:**
{
  "word": "water",
  "is_common": true,
  "reasoning": "'Water' is one of the most fundamental words in the English language, learned in early childhood and used daily by everyone. It is essential for everyday life and basic education.",
  "category": "General"
}

[Example 2]

**Input:** osmosis

**Output:**

{
  "word": "osmosis",
  "is_common": false,
  "reasoning": "While taught in high school biology, 'osmosis' is a specific scientific term not used in everyday conversation. Its usage is almost exclusively confined to academic or educational contexts.",
  "category": "Biology"
}

[Example 3]

**Input: anomie**

**Output:**

{
  "word": "anomie",
  "is_common": false,
  "reasoning": "This is a specialized term from sociology referring to a lack of usual social or ethical standards in an individual or group. It is not known or used by the general population.",
  "category": "Social Sciences"
}


[Example 4]

**Input:** forsooth

**Output:**

{
  "word": "forsooth",
  "is_common": false,
  "reasoning": "This is an archaic word for 'indeed' or 'in truth'. It is no longer part of modern English and is only encountered in historical texts or for literary effect.",
  "category": "Archaic / Obsolete"
}

[Example 5]

**Input:** algorithm

**Output:**

{
  "word": "algorithm",
  "is_common": false,
  "reasoning": "Although the concept is increasingly discussed in news and media, the word 'algorithm' is still technical. Many people may have heard it but lack a precise understanding, and it is not used in most daily, non-technical conversations.",
  "category": "Technology / Computer Science"
}

[Example 6]

**Input:** thank you
**Output:**

{
  "word": "thank you",
  "is_common": true,
  "reasoning": "This is a core polite phrase used universally in daily interactions. It is one of the first phrases taught to children and is a cornerstone of everyday communication.",
  "category": "General"
}

"""

def check_regularity(word: str) -> Regularity:
    response = get_chat_response(instruction, f'[Your Turn]: input: {word} output: ')
    return Regularity.model_validate_json(response)

if __name__ == '__main__':
    result = check_regularity('rizz')
    print(result.word)
    print(result.reasoning)
    print(result.category)