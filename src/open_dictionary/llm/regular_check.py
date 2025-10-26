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
    SLANG_INFORMAL = "Slang / Informal"


class Regularity(BaseModel):
    category: Category
    tags: list[str]
    commonness_score: int

instruction = """

You are an expert lexicographer and data classifier, tasked with categorizing English word definitions for a large-scale dictionary project. Your analysis must be precise, consistent, and adhere strictly to the provided format.

**Your Task:**
1.  Analyze the provided word, its part of speech (POS), and its English definition.
2.  Based ONLY on the information in the definition, assign it to the most appropriate primary category and any relevant secondary tags.
3.  Estimate the "commonness" of the word itself in general English usage on a scale of 1 to 10.

**Rules:**
1.  **Primary Category:** You MUST choose exactly ONE primary category from the `Category List` that BEST fits the definition.
2.  **Secondary Tags:** You MAY choose up to THREE secondary tags from the `Category List`. Tags should represent other relevant fields but must not be the same as the primary category. If no other fields are relevant, provide an empty list: `[]`.
3.  **Commonness Score (`commonness_score`):** You MUST provide an integer score from 1 to 10.
    *   **10:** Core function words. The absolute building blocks of English (e.g., `the`, `a`, `of`, `is`, `i`, `you`).
    *   **8-9:** Extremely common content words used in daily conversation by nearly everyone (e.g., `house`, `walk`, `happy`, `water`, `good`).
    *   **6-7:** Common, everyday words, but slightly more descriptive or specific (e.g., `market`, `decision`, `beautiful`, `sophisticated`).
    *   **4-5:** Educated vocabulary. Words understood by most educated adults but not used daily (e.g., `ubiquitous`, `elaborate`, `criterion`).
    *   **2-3:** Specialized or formal vocabulary. Common in academic, legal, or technical fields, but rare in general conversation (e.g., `jurisprudence`, `ionize`, `game theory`).
    *   **1:** Very rare, archaic, obsolete, or extremely technical jargon (e.g., `weltanschauung`, `afeard`, `syzygy`).
4.  **Scoring Focus:** The `commonness_score` applies to the **word itself**, not its specific definition. For example, the word "culture" is very common (score 9), even if one of its definitions is a technical one from biology.
5.  **Output Format:** Your final output MUST be a single, valid JSON object and nothing else. Do not include any explanations, greetings, or surrounding text.

**Category List:**
- General
- Biology
- Chemistry
- Physics
- Mathematics
- Technology / Computer Science
- Medicine
- Social Sciences
- Humanities
- Business & Economics
- Law
- Arts & Culture
- Archaic / Obsolete
- Slang / Informal

--- EXAMPLES ---

---
Input Data:
{
  "word": "semiconductor",
  "pos": "noun",
  "definition": "A substance, such as silicon, that has electrical conductivity intermediate between that of a conductor and an insulator, used in the manufacture of electronic devices."
}

Your JSON Output:
{
  "category": "Technology / Computer Science",
  "tags": ["Physics", "Chemistry"],
  "commonness_score": 4
}
---
Input Data:
{
  "word": "culture",
  "pos": "noun",
  "definition": "The cultivation of bacteria, tissue cells, etc., in an artificial medium containing nutrients."
}

Your JSON Output:
{
  "category": "Biology",
  "tags": ["Medicine"],
  "commonness_score": 9
}
---
Input Data:
{
  "word": "walk",
  "pos": "verb",
  "definition": "To move at a regular pace by lifting and setting down each foot in turn, never having both feet off the ground at once."
}

Your JSON Output:
{
  "category": "General",
  "tags": [],
  "commonness_score": 9
}
---
Input Data:
{
  "word": "game theory",
  "pos": "noun",
  "definition": "The mathematical study of strategic decision making, applied in contexts of conflict and cooperation."
}

Your JSON Output:
{
  "category": "Mathematics",
  "tags": ["Social Sciences", "Business & Economics"],
  "commonness_score": 3
}
---
Input Data:
{
  "word": "afeard",
  "pos": "adjective",
  "definition": "(Dialectal or archaic) Afraid, scared, frightened."
}

Your JSON Output:
{
  "category": "Archaic / Obsolete",
  "tags": [],
  "commonness_score": 1
}
---
Input Data:
{
  "word": "rizz",
  "pos": "noun",
  "definition": "(Internet slang) Style, charm, or attractiveness; the ability to attract a romantic or sexual partner."
}

Your JSON Output:
{
  "category": "Slang / Informal",
  "tags": [],
  "commonness_score": 3
}
---

"""

def check_regularity(word: str, pos: str, definition: str) -> Regularity:
    import json
    input_data = json.dumps({"word": word, "pos": pos, "definition": definition})
    response = get_chat_response(instruction, f'--- TASK ---\n\nInput Data:\n{input_data}\n\nYour JSON Output:')
    return Regularity.model_validate_json(response)

if __name__ == '__main__':
    result = check_regularity('rizz', 'noun', 'An amount of rolling paper particularly of the Rizla+ brand.')
    print(result.category)
    print(result.tags)
    print(result.commonness_score)