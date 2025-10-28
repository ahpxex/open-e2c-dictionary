# Open English -> Chinese Dictionary

开源的、由大模型构建的庞大英汉词典，尤其注重详细的中文解释，更容易获取到词语的微妙含义。

- 推荐前端实现：[Aictionary](https://github.com/ahpxex/aictionary)

---

## 项目结构

```
open-e2c-dictionary
├── dictionary/               # 25,318 个 JSON 词条（[单词名称].json）
├── lib/query.py              # 调用大模型并生成 JSON 的核心逻辑
├── main.py                   # 批量生成词条的入口脚本
├── check_json_structure.py   # 校验 JSON 结构是否符合 system prompt
├── generate_json_template.py # 汇总所有词条中出现过的字段模板
├── clean_json_entries.py     # 清理词条中空字段或空对象
├── words.txt                 # 构建词库所使用的词频列表
├── README.md                 # 原始简要介绍
└── README.enhanced.md        # 本文（增强版说明）
```

---

## 快速开始

**获取数据**

- 直接克隆仓库，或在 [Release](https://github.com/ahpxex/open-e2c-dictionary/releases) 页面下载最新 zip 包。

---

## 数据样例

在本项目的 `dictionary` 目录下，存放了 25,318 个这样的 JSON 文件，每个 JSON 文件严格按照 `[单词名称].json` 的格式来命名。

你也可以在本项目的 [Release](https://github.com/ahpxex/open-e2c-dictionary/releases) 界面下载到最新的 zip 压缩包，应用在你的项目当中。

```json
{
  "word": "hello",
  "pronunciation": "huh·loh",
  "concise_definition": "interj. 你好, 喂, 问候语",
  "forms": {
    "plural": "hellos"
  },
  "definitions": [
    {
      "pos": "interjection",
      "explanation_en": "A common greeting used to acknowledge someone's presence, initiate a conversation, or answer a phone call.",
      "explanation_cn": "一种常见的问候语，用于表示对某人出现的注意、开启对话或接听电话。",
      "example_en": "Hello! How are you today?",
      "example_cn": "你好！今天过得怎么样？"
    },
    {
      "pos": "interjection",
      "explanation_en": "An expression used to attract attention, especially in noisy or distant situations, or to express surprise or skepticism.",
      "explanation_cn": "用于引起注意的感叹词，尤其在嘈杂环境或距离较远时，也可用于表达惊讶或怀疑。",
      "example_en": "Hello? Is anyone there? I can't hear you!",
      "example_cn": "喂？有人在吗？我听不见你！"
    }
  ],
  "comparison": [
    {
      "word_to_compare": "hi",
      "analysis": "“Hi” 是 “hello” 的非正式缩略形式，语气更随意、轻松，常用于朋友或熟人之间。而 “hello” 更中性，适用于正式与非正式场合，也更常用于电话问候。"
    },
    {
      "word_to_compare": "greetings",
      "analysis": "“Greetings” 是复数名词，泛指各种问候方式（如 good morning, how do you do），语气更正式或书面化，通常用于贺卡或官方信函。而 “hello” 是一个具体的、口语化的问候语，使用频率更高、更直接。"
    },
    {
      "word_to_compare": "hey",
      "analysis": "“Hey” 是一种更随意、甚至略显粗鲁的打招呼方式，常用于年轻人之间或非正式场合，有时带有打断或召唤的语气。相比之下， “hello” 更礼貌、中性，适用于更广泛的社交语境。"
    }
  ]
}
```

---

## 技术实现

现有的词典条目由在 4 张 4090 显卡的计算服务器上运行的 `Qwen/Qwen3-Next-80B-A3B-Instruct` 大模型构建，所有的条目均使用 `lib/query.py` 中的 system prompt 来生成，如果你需要构建自己的词库，可以先按照 `.env.example` 中的实例，配置好大模型服务提供商，然后运行 `main.py` 即可。

### System Prompt 要点

- 所有字段必须填写完整，禁止留空。
- JSON 中如需使用引号，必须使用中文双引号（“ ”）。
- 参考 prompt 内置的两个示例，确保结构与深度一致。

---

## 数据质量与维护工具

| 脚本                        | 作用                                                                                       | 用法提示                               |
| --------------------------- | ------------------------------------------------------------------------------------------ | -------------------------------------- |
| `check_json_structure.py`   | 校验所有词条是否符合 system prompt 结构，若发现违规词条，可选择删除后重新生成。            | `uv run check_json_structure.py`       |
| `generate_json_template.py` | 根据现有词条推导出包含全部出现过的字段的模板，辅助扩展或对齐结构。                         | `uv run generate_json_template.py`     |
| `clean_json_entries.py`     | 清除词条中空的键值对，若对象/数组因此为空则整体删除。默认 dry-run，可配合 `--apply` 落盘。 | `uv run clean_json_entries.py --apply` |

> 建议在提交前按顺序执行：`clean_json_entries.py --apply` → `check_json_structure.py`，确保数据干净可靠。

---

## 再生产流程建议

1. **准备词频表**：更新或替换 `words.txt`。
2. **运行生成**：执行 `uv run main.py`。
3. **格式清理**：可选运行 `uv run clean_json_entries.py --apply`。
4. **结构校验**：运行 `uv run check_json_structure.py`，依据提示处理异常。
5. **模板更新（可选）**：`uv run generate_json_template.py`，观察新增字段是否合理。
6. **打包发布**：确认字典目录无误后提交 PR 或发布新 Release。

---

## 常见问题

- **Q: 词条里出现了英文双引号怎么办？**  
  A: 检查生成时的 prompt 或手动替换为中文双引号，必要时重新生成该词条。

- **Q: 如何定位结构不符的词条？**  
  A: `check_json_structure.py` 会列出具体文件及差异项。

- **Q: 可以扩展更多字段吗？**  
  A: 可以，但请先确认模型及前端是否兼容，并在模板与 README 中同步说明。

---

## 贡献指南

1. Fork 仓库并创建新分支。
2. 完成修改后运行清理与校验脚本，确保 JSON 合规。
3. 提交 PR 时简要描述修改内容、影响范围、是否重新生成词条。
4. 对新增或修改字段，建议在 `generate_json_template.py` 结果与 README 中同步说明。

---

## 致谢

- [wordfreq](https://github.com/rspeer/wordfreq) 提供的高质量词频数据。
- [Qwen 团队](https://qwen.ai) 的开放模型支持。
- 所有提交 issue、PR 以及在社区推广项目的贡献者。

---

## 许可证

本项目采用与原仓库一致的许可证（请参阅根目录中的 `LICENSE` 文件，如未提供则根据使用需求补充说明）。在使用词典数据时，请确保遵守相应的条款与当地法律法规。

---

欢迎在 GitHub 上提出建议、提交 PR，或将你的应用示例分享到社区，一起完善这份英汉词典。谢谢！ 🎉
