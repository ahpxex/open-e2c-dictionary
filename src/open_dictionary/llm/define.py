from pydantic import BaseModel
from typing import Optional
import json
from open_dictionary.llm.llm_client import get_chat_response


instruction = """
你是一位精通中英双语的词典编纂专家。
**任务**：读取输入JSON，严格按照【输出规则】生成一个JSON对象作为唯一结果，无任何额外内容。

---
**【输出规则】**

1.  `word`, `pos`: 直接提取。
2.  `pronunciations`: 提取 `ipa` 和 `ogg_url` (若无则为`null`)。生成 `natural_phonics` (自然拼读，如 'phi-lo-so-phy')。
3.  `forms`: 处理 `forms` 数组，格式为 "词形 (中文说明)"，例如 "hits (第三人称单数现在时)"。
4.  `concise_definition`: 总结所有词义，生成一个核心中文释义。
5.  `detailed_definitions`: 遍历 `senses` 数组，为每个词义生成对象：
    *   `definition_en`: 提取最具体的英文释义。
    *   `definition_cn`: 用通俗中文**阐释**其精髓和用法，**不要生硬直译**。
    *   `example`: **创作一个全新的、简单的现代中英例句**，忽略原始例句。
6.  `derived`: 遍历 `derived` 数组，为每个派生词生成简明中文定义。
7.  `etymology`: 将 `etymology_text` **转述**为流畅的中文词源故事。

---

**【示例 1】**

**输入JSON:**
`{"word": "run", "pos": "verb", "forms": [{"form": "runs", "tags": ["present", "singular", "third-person"]}, {"form": "running", "tags": ["participle", "present"]}, {"form": "ran", "tags": ["past"]}, {"form": "run", "tags": ["participle", "past"]}], "senses": [{"glosses": ["To move swiftly on foot, so that both feet leave the ground during each stride."]}, {"glosses": ["To manage, be in charge of (a business, campaign, etc.)."]}], "sounds": [{"ipa": "/rʌn/", "ogg_url": "https://.../En-us-run.ogg"}], "derived": [{"word": "runner"}, {"word": "runway"}], "etymology_text": "From Middle English ronnen, rennen, from Old English rinnan, iernan ("to run, flow"), and Old Norse rinna ("to run, flow"), both from Proto-Germanic *rinnaną ("to run, flow")."}`

**你的JSON输出:**
{
  "word": "run",
  "pos": "verb",
  "pronunciations": {
    "ipa": "/rʌn/",
    "natural_phonics": "run",
    "ogg_url": "https://.../En-us-run.ogg"
  },
  "forms": [
    "runs (第三人称单数现在时)",
    "running (现在分词)",
    "ran (过去式)",
    "run (过去分词)"
  ],
  "concise_definition": "跑，奔跑；经营，管理。",
  "detailed_definitions": [
    {
      "definition_en": "To move swiftly on foot, so that both feet leave the ground during each stride.",
      "definition_cn": "指人或动物快速奔跑的动作，强调速度快、双脚交替离地的状态。",
      "example": {
        "en": "I run three miles in the park every morning.",
        "cn": "我每天早上在公园里跑三英里。"
      }
    },
    {
      "definition_en": "To manage, be in charge of (a business, campaign, etc.).",
      "definition_cn": "指负责管理和运营一个组织、项目或生意，强调的是主导和控制的角色。",
      "example": {
        "en": "My friend runs a successful online store.",
        "cn": "我的朋友经营着一家成功的网店。"
      }
    }
  ],
  "derived": [
    {
      "word": "runner",
      "definition_cn": "跑步者；赛跑者。"
    },
    {
      "word": "runway",
      "definition_cn": "（机场的）跑道；（时装秀的）T台。"
    }
  ],
  "etymology": "该词源自中古英语中的 ronnen 和 rennen，更早则来自古英语的 rinnan（意为\"奔跑，流动\"）和古诺斯语的 rinna（意为\"奔跑，流动\"）。它们共同源于原始日耳曼语的 *rinnaną，核心含义是\"奔跑或流动\"。"
}

"""


class Example(BaseModel):
    en: str
    cn: str


class DetailedDefinition(BaseModel):
    definition_en: str
    definition_cn: str
    example: Example


class DerivedWord(BaseModel):
    word: str
    definition_cn: str


class Pronunciations(BaseModel):
    ipa: str
    natural_phonics: str
    ogg_url: Optional[str] = None


class Definition(BaseModel):
    word: str
    pos: str
    pronunciations: Pronunciations
    forms: list[str]
    concise_definition: str
    detailed_definitions: list[DetailedDefinition]
    derived: list[DerivedWord]
    etymology: str


def define(input_json: dict) -> Definition:
    """Generate a structured dictionary definition from Wiktionary JSON data.

    Args:
        input_json: Dictionary containing Wiktionary data

    Returns:
        Definition object with structured dictionary entry
    """
    input_data = json.dumps(input_json, ensure_ascii=False)
    response = get_chat_response(instruction, input_data)
    print(response)
    return Definition.model_validate_json(response)

de = define({'word':'tractor'})
print(de)