# decisions

- Fileについて
  - Upload時、FileはBlobStorageにブロック毎にコピーされる
  - Fileはハッシュ値で検索する
  - 検索情報はFileRefと呼ぶ
  - ユーザーがUploadしたFileと、システムが内部でUploadしたファイルと区別できるようにする
    - たとえば、掲示板機能を提供する場合、Fileとして掲示板情報を格納し、MemoにはFileのリンク情報を格納する
- Memoについて
  - MemoはValue(Bytes)と証明書情報を持つ
  - Memoは`signature/tag`の空間で検索する
  - 検索情報はMemoRefと呼ぶ
