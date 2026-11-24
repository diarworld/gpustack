---
translation_status: translated
---

# レート制限 { #rate-limiting }

GPUStack は、トラフィックがモデルインスタンスに到達する前に、APIゲートウェイでユーザーごとのリクエストレートおよびトークン消費量の上限を強制できます。上限は Higress ゲートウェイ上で動作する `gpustack-rate-limit` プラグインによって強制され、カウンターは Redis に保持されるため、すべてのゲートウェイレプリカにわたって有効です。

## 動作の仕組み { #how-it-works }

- **リクエストレート（RPM）** — リクエスト数で計上される、1分ごとのスライディングウィンドウ。
- **トークンレート（TPM）** — 各リクエストが消費するトークン数で計上される、1分ごとのスライディングウィンドウ。
- **月次トークン予算** — デプロイのタイムゾーンで毎月1日にリセットされる、暦に沿った許容量。
- **週次トークン予算** — 直近7日間のローリングウィンドウ。
- **モデルあたりの同時リクエスト数** — 1つのモデルに対して同時に実行できるユーザーのリクエスト数の上限。リクエストの実行中はスロットが取得され、ストリームの終了時に解放されます。

上限を超えたリクエストは、モデルインスタンスに接続される前に、OpenAI エラーフォーマットの `429 Too Many Requests` としてゲートウェイで拒否されます。拒否レスポンスの本文は、発動した上限を具体的に示します —
`Too many requests per minute`, `Weekly token quota limit reached, reset in
usage overview`, `Too many concurrent requests for this model` — という具合です。ローリング
および暦ウィンドウには、ウィンドウが空くまでの秒数を示す `Retry-After` ヘッダーが付きます（同時実行の拒否には付きません。スロットは固定時刻ではなく、実行中のリクエストが完了したときに解放されます）。拒否レスポンスには、拒否を引き起こした Redis キーの名前を示す `x-ratelimit-limited-key` ヘッダーも含まれ、429 をその発生元のユーザーとウィンドウに紐付けることができます。

上限は**ユーザーごと**にバケット化されます。ユーザーのAPIキーのトラフィックとUIのトラフィックは、同じ許容量を消費します。レート制限の対象になるのは、推論パス（標準の OpenAI/Anthropic エンドポイントと `/model/proxy`）のみです。

## デプロイ全体のデフォルト { #deployment-wide-defaults }

デフォルトは、独自の明示的な上限を持たないすべての利用者に適用されます。新規デプロイでは、サーバー設定ファイル（サーバーが `--config-file` または `GPUSTACK_CONFIG_FILE` 環境変数で読み込む YAML ファイル）から初期値が取り込まれます:

```yaml
gateway_rate_limit:
  enabled: true
  # Redis is strongly recommended in production: without it the plugin keeps
  # counters per gateway replica, and a multi-replica gateway enforces roughly
  # limit x replicas.
  redis:
    service_name: redis.gpustack-system.svc.cluster.local
    service_port: 6379
  timezone: UTC
  default_rpm: 3
  default_tpm: 768000
  default_monthly_tokens: 100000000
  default_concurrency: 1
```

以降、デフォルトはデータベースに保存され、「Usage」→「Limits」タブ（**デプロイのデフォルト**）で編集されます。こちらが設定ファイルより優先されます — 設定ファイルはブートストラップのシードとして、およびフォームで空欄のままにされたフィールドのソースとして引き続き機能します。フォームの空欄（データベースでは `null`）は設定ファイルの値を継承するため、デプロイは再起動なしでデフォルトをファイル側に戻すことができます。

`default_concurrency` を除くすべての `default_*` フィールドは任意です。未設定のフィールドは、その種類の上限がないことを意味します。`default_concurrency` のデフォルトは **1** です — ユーザーあたり・モデルあたり1件の実行中リクエスト — これは、数十の並列ストリームを握り続ける暴走したバッチクライアントこそが、この上限が存在する理由である障害モードだからです。`0` を設定すると、デプロイ全体で同時実行上限が無効になります。その他のフィールドは設定されるまでパススルーのままです。`rejected_code` / `rejected_msg` はデフォルトでは存在せず、プラグインの上限ごとの拒否テキストが適用されます。明示的な値を設定すると上書きされます。

### フリーウィンドウ { #free-windows }

同じフォームが、デプロイ全体の**フリーウィンドウ**を公開します。毎日の時間帯（深夜0時をまたげます）と終日フリーの曜日の組み合わせです。スケジュールはプラグインの `free_windows` 設定に乗り、プラグイン全体の `timezone` で評価されます:

```yaml
free_windows:
  enabled: true
  start: "15:00"        # HH:MM in the plugin-wide timezone
  end: "06:00"          # may wrap past midnight
  days: ["sat", "sun"]  # mon..sun; empty = the daily window every day
```

スケジュール内では、プラグインはすべてのチェックを停止します。そこで許可されたリクエストには上限が適用されず、同時実行スロットも取得せず、カウンターにも何も加算されません — ウィンドウ**内で開始**されたリクエストは、終了時刻を過ぎてストリーミングが続く場合でも、全体が無料になります。`ai-statistics` からの使用統計は影響を受けません。`gpustack_rate_limit_free_skipped_total` メトリクスがバイパスされたリクエストをカウントします。UIはスケジュールをブラウザのタイムゾーンで表示します。保存される値（および曜日）は UTC の実時刻です。

Redis にはゲートウェイの Pod から到達できる必要があります。ゲートウェイにサービスを提供する McpBridge で宣言してください（または、Higress の設定で `global.onlyPushRouteCluster: false` を設定した後、`service_name` を Kubernetes Service に向けてください）。

## ユーザーごとの上限 { #per-user-limits }

明示的な上限はデプロイのデフォルトを上書きします。これらはユーザーアカウントに設定されます — `rpm_limit`、`tpm_limit`、`weekly_token_limit`、`monthly_token_limit` — であり、ユーザーのすべてのAPIキーとUIセッションで共有されます。

週次予算にはデプロイ全体のデフォルトがありません。明示的な週次予算を持つアカウントのみがこの上限の対象になります。明示的な上限のないユーザーは、所属グループのクォータとデプロイのデフォルトに従います — [グループクォータ](#group-quotas)を参照してください。

### モデル別の同時実行 { #per-model-concurrency }

`default_concurrency` のデプロイのデフォルト（デプロイが変更しない限り 1）は、1人のユーザーが**1つのモデル**に対して同時に実行できるリクエスト数を制限します。最初のリクエストがまだストリーミング中の間に同じモデルへ送られた2件目の並行リクエストは 429 で拒否されますが、別のモデルへのリクエストは独立して進みます。カウントの対象は実行中のリクエスト数であり、レートではありません — リクエストがゲートウェイを通過するときにスロットが取得され、ストリームの終了時に解放されるため、完了したリクエストは直ちにスロットを解放します。

管理者は同時実行上限の対象外です。モデル別の上書きは、`model_concurrency_limits` でユーザーアカウントに設定します — モデル名をそのアカウントにおけるそのモデルの上限に対応付けるマップで、`0` は明示的な無制限を意味します:

```json
{
  "model_concurrency_limits": {
    "glm-5.3-flash": 4,
    "qwen3.8-27b": 2,
    "text-embedding-model": 0
  }
}
```

マップに存在しないモデルは `default_concurrency` を継承します。管理者はユーザー設定ドロワーでユーザーごとにこのマップを編集できます。変更は1リコンサイル間隔以内（デフォルトで30秒）に反映されます。

!!! note

    解放が失われたスロット — ストリーム中のゲートウェイクラッシュ — は、1時間後に自動的に回収されます（プラグインの `concurrency_stale_seconds`、`gateway_rate_limit.plugin_config` で調整可能）。そのため、クラッシュしたストリームがユーザーのスロットを永久にロックし続けることはありません。

変更は1リコンサイル間隔以内（デフォルトで30秒。`GPUSTACK_GATEWAY_RATE_LIMIT_RECONCILE_INTERVAL_SECONDS` を参照）に反映されます。

!!! note

    レート制限はゲートウェイで強制されるため、ゲートウェイを経由して到着するトラフィックに適用されます。サーバーのAPIポートに直接送られる推論呼び出しには、レート制限は適用されません。

## グループクォータ { #group-quotas }

グループはユーザーアカウントと同じ上限フィールド — RPM、TPM、週次・月次トークン予算、モデル別の実行中リクエスト上限 — を持ち、「Access Control」→「Groups」から **Manage limits** で設定します。グループの上限がグループを単位として制限することはありません。各メンバーの実効上限は、次の優先順位で**フィールドごとに解決**されます:

1. ユーザーの明示的な上限 — アカウントに設定されており、どちらの方向にもこれが優先されます。
2. なければ、ユーザーが所属するグループ（ネストされたサブグループも含む）全体での最小値。グループの上限はデプロイのデフォルトを締め付けることも緩めることもできます — 他のチームより高いRPMを1チームに与えることは正当な使い方です。
3. なければ、デプロイ全体のデフォルト。

たとえば、`default_rpm: 100` の環境で、あるグループがRPM 10に、別のグループがRPM 2に上限を設けている場合、明示的な上限のないユーザーはRPM 2で実行されます。同じグループがTPMを1000と10000に上限設定しているなら、TPMは1000になります。各フィールドは独立して解決されます。アカウントで明示的にRPM 30を設定していれば、グループの設定にかかわらず 30 が適用されます。週次予算にはデプロイ全体のデフォルトがないため、ユーザーの行、グループの最小値、または無制限のいずれかから決まります。

グループレベルのモデル別上限は、1モデルずつ解決されます。ユーザー自身の `model_concurrency_limits` エントリが優先され（保存された `0` は明示的な無制限）、なければメンバーが所属するグループ全体での最小の非ゼロの上限が適用され、それもなければ `default_concurrency` が適用されます。**グループ**の `0` は「このグループはそのモデルを制限しない」を意味します。比較に一切入らないため、デプロイのデフォルトを締め付けることも、兄弟グループの上限を緩めることもありません。

解決はサーバー側で行われます。ゲートウェイのプラグインは、解決済みの数値を含むユーザーごとの1つの組み合わせを受け取り続けるだけで、グループを一切見ません。クォータの編集やメンバーシップの変更（ユーザーのグループへの参加・脱退）は、1リコンサイル間隔以内（デフォルトで30秒）に取り込まれます。「Usage」ページと `GET /v2/usage/rate-limits` エンドポイントは、実際に強制される数値である解決済みの値を報告します。

## 障害時の挙動 { #failure-behaviour }

レートリミッターはフェイルオープンです。Redis に到達できない場合、リミッター自身のインフラを理由にトラフィックを遮断するのではなく、リクエストは通過を許可され、警告がログに記録されます。上限が強制されている証拠としては、後述のメトリクスセクションの拒否カウンターを監視してください。

## 現在の上限と使用量 { #current-limits-and-usage }

サーバーは、ゲートウェイが現在实际に強制している内容を `GET /v2/usage/rate-limits` で公開しています。認証済みのユーザーはデフォルトで自分自身の状況を取得します。プラットフォーム全体の管理者（Org をピン留めせずに操作する管理者 — 使用状況ルートがテナント横断的な表示に適用するのと同じゲート）は `user_id` を渡して任意のユーザーを表示できますが、それ以外のユーザーは自分自身の表示になります。

レスポンスには以下が含まれます:

- `mode` — `redis`（ライブカウンター。リクエスト時に Redis から読み取る）、`local`（Redis なしでレート制限が有効。カウンターなしで上限が返る）、または `disabled`（機能が無効。「Usage」ページではセクションが非表示）。
- `user` — ユーザーの週次・月次トークン予算（設定されているもの）。それぞれにライブウィンドウが付きます。さらに `model_concurrency` — アカウントに設定されたモデル別の実行中リクエスト上限（`model_concurrency_limits`）。それぞれにライブの `running` スロット数が付きます。明示的に名前が挙げられたモデルのみが列挙され、管理者（対象外）と上書きのないユーザーには空のリストが返ります。`concurrency_default` には他のモデルが継承するデプロイ全体の上限が入ります（上限が無効な場合は `null`）。
- `generated_at` — すべてのウィンドウが読み取られた時刻。
- `usage_error` — Redis の読み取りが失敗したときにのみ設定され、例外のクラス名のみを運びます。メッセージ（ホスト名を含みうる）は決して含まれません。

値のないフィールドは `null` として送られるのではなく省略されます。`local` モードや Redis に到達できない場合は、単純にウィンドウに `used` がありません。読み取りはフェイルオープンです — 上限は常に返されます。「Usage」ページの「Summary」タブは、このレスポンスを **Rate limits** セクションとして表示します。

デフォルトがRPM 3、TPM 768000、月次トークン 100000000 のデプロイで、週次予算 100000 のユーザーの場合:

```json
{
  "mode": "redis",
  "generated_at": "2026-11-19T10:30:00Z",
  "user": {
    "weekly_limit": 100000,
    "concurrency_default": 1,
    "windows": [
      {"kind": "query", "period": "60s", "window_seconds": 60, "limit": 3, "used": 2, "reset_seconds": 41},
      {"kind": "token_rolling", "period": "60s", "window_seconds": 60, "limit": 768000, "used": 4820, "reset_seconds": 27},
      {"kind": "token_calendar", "period": "each_month", "limit": 100000000, "used": 210000, "reset_seconds": 999000},
      {"kind": "token_rolling", "period": "604800s", "window_seconds": 604800, "limit": 100000, "used": 37215, "reset_seconds": 172800}
    ],
    "model_concurrency": []
  }
}
```

すべてのウィンドウはユーザーを単位としてバケット化されます。APIキーのトラフィックとUIのトラフィックは同じ許容量を消費するため、両者は同じカウンターを読みます。暦ウィンドウには `window_seconds` がありません — 長さは暦に従い、`reset_seconds` はデプロイのタイムゾーンにおける境界までのカウントダウンです。

### カウンターの復旧 { #counter-recovery }

プラグインの Redis カウンターが唯一の強制状態です。レート制限用の Redis がカウンターを失うと — 新規インスタンス、フェイルオーバー、永続化なしのフラッシュ — 暦クォータはゼロから再開し、日次・週次・月次の予算をすでに使い切ったユーザーは、ウィンドウの残りの期間で予算を取り戻します。この隙間を埋めるため、サーバーは `model_usage_details` にすでに記録した消費を、プラグインが読み取るのと同じカウンターへ再生します。対象はまだ存在しないカウンターのみで、ライブのカウンターには決して触れません。この処理はループで実行されます（5分ごと）。各サイクルは1回のパイプライン化された EXISTS でカウンターを調べ、実際に欠落があるときだけ詳細のスキャンが実行されるため、Redis のフラッシュは1間隔以内で自己修復します。

意図的に正確ではありません:

- 分単位のウィンドウは再構築されません — 60秒でエージアウトし、再構築が意味を持つ前に消えます。
- 再構築される要素は1時間ごとの集計値のため、スライディングする週はリクエスト単位ではなく1時間単位で減衰します。
- プラグインはゲートウェイでカウントし、サーバーはプロキシで記録するため、中断または未追跡のリクエストによって、再構築された合計が失われた合計からわずかにずれることがあります。

レート制限用の Redis で永続化（AOF / RDB）を有効にすれば、再構築自体を回避できます。

!!! note

    このエンドポイントと再構築処理がやり取りするのは、ゲートウェイがカウンターを書き込むのと同じ Redis — `gateway_rate_limit.redis` — であり、サーバーのトップレベルの `redis_url` ではありません。

### 予算のリセット { #resetting-a-budget }

「Usage」ページの週次・月次予算カードにはリセットボタンがあります。押すと、そのユーザーの該当ウィンドウ種別のカウンターが削除され、自然な境界を待たずに予算がウィンドウの途中からゼロへやり直します。押下のたびに `gateway_rate_limit_resets` に記録されます — 理由のメモは必須で、ユーザー、押下日、管理者が代理で押した場合はその実行者とともに行に保存されます。

ウィンドウごとに2回の押下は無料で、回数は押下そのものに応じて「浮動」します — 同じ週に2回リセットすると、次のリセットは次の暦の境界ではなく、最初の押下から1週間後に可能になります。2回とも使い切ると、最も古い押下がウィンドウから抜けるまでボタンは無効化されます。プラットフォーム全体の管理者はこの回数制限を受けません。管理者による押下（自分の予算でも他のユーザーの予算でも）は記録されますが、ユーザー自身の無料枠を消費することはありません。

カウンターの再構築はリセットと連携します。該当ウィンドウ種別の最新のリセットより前に記録された使用量は帳消しになるため、Redis の喪失後の再構築は、帳消しにされた消費を復活させるのではなく、リセットの時点から再開します。

フリーウィンドウのスケジュールも同じように尊重されます。プラグインはウィンドウ内のリクエストをカウントせずに許可するため、スケジュールが有効になった（設定行の作成）後に記録された行のうち、リクエストがウィンドウ内で開始されたものは再生から除外されます — さもなければ、サーバーを再起動するたびに無料の時間が予算に請求し戻されることになります。無効なスケジュールは何も除外しません。

## メトリクス { #metrics }

プラグインはレート制限の判断ごとに3つのカウンターを宣言します:

- `gpustack_rate_limit_request_total` — 上限チェックが実行されたリクエスト。`result="passed"` と `result="limited"` に分けられます。
- `gpustack_rate_limit_rejected_total` — 拒否されたリクエスト。拒否を引き起こした組み合わせとウィンドウに帰属します。
- `gpustack_rate_limit_value_total` — 各ウィンドウのバケットに書き込まれた消費量（クエリウィンドウと同時実行の取得ではリクエストあたり 1、トークンウィンドウではトークン数）。

Higress の AI statistics プラグインはこれらをルートレベルの Envoy メトリクスへ融合させるため、ゲートウェイの Prometheus エンドポイントでは `route_upstream_model_consumer_metric_` プレフィックスで公開され、組み合わせとウィンドウは名前にフラット化され、AI の次元はラベルとして付与されます:

```promql
route_upstream_model_consumer_metric_gpustack_rate_limit_request_total_rule_gpustack_rate_limit_result_passed{ai_route="ai-route-route-1.internal",ai_cluster="outbound_80__model-1-46.static",ai_model="glm-5.3-flash",ai_consumer="a3d73e9f0c402120.gpustack-6"}
```

- `ai_consumer` — 認証された利用者。例: `a3d73e9f0c402120.gpustack-6`。
- `ai_route`、`ai_cluster`、`ai_model` — リクエストの宛先だった AI ルートとモデルインスタンス。
- 組み合わせとウィンドウは名前のセグメントになります（英数字以外は `_` にフラット化）:
  `..._rejected_total_rule_gpustack_rate_limit_combo_<combination>_kind_<query|token_rolling|token_calendar>_period_<60s|604800s|each_day|each_month>_bucket_consumer_<consumer>`。

カウンターはゲートウェイの Pod が `:15020/stats/prometheus` でエクスポートします。Pod には標準の `prometheus.io/scrape: "true"`、`prometheus.io/port: "15020"`、`prometheus.io/path: "/stats/prometheus"` アノテーションが付いているため、Pod のアノテーションをスクレイプする Prometheus はこれらを自動的に取り込みます。

### リラベリングによる名前の短縮 { #shortening-the-names-with-relabeling }

Envoy の統計にはラベルの次元がありません — プラグインはそれらを統計名にエンコードすることしかできず、AI のスケルトンプレフィックスこそが、Higress の組み込みブートストラップエクストラクターに `ai_*` ラベルを生成させるものです。クラスタ側のエクストラクター設定はこの Higress バージョンでは固定です（EnvoyFilter の BOOTSTRAP パッチは反映されません）。そのため、id をラベルにした短い名前を得る方法は、Prometheus 側の `metric_relabel_configs` です。ゲートウェイのスクレイプジョブに適用すると、上記の名前が `rl_combo` / `rl_kind` / `rl_period` / `rl_bucket` / `rl_result` ラベル付きの `gpustack_rate_limit_{request,rejected,value}_total` に変わります:

```yaml
metric_relabel_configs:
  # ids out of the flattened name, into labels — these rules read the
  # ORIGINAL name, so they must come before the rename below.
  - source_labels: [__name__]
    regex: .*gpustack_rate_limit_request_total_rule_.*_result_(.+)
    target_label: rl_result
  - source_labels: [__name__]
    regex: .*_combo_(.+?)_kind_.*
    target_label: rl_combo
  - source_labels: [__name__]
    regex: .*_kind_(.+?)_period_.*
    target_label: rl_kind
  - source_labels: [__name__]
    regex: .*_period_(.+?)_bucket_consumer_.*
    target_label: rl_period
  - source_labels: [__name__]
    regex: .*_bucket_consumer_(.+)
    target_label: rl_bucket
  # shorten the name last
  - source_labels: [__name__]
    regex: route_upstream_model_consumer_metric_(gpustack_rate_limit_(?:request|rejected|value)_total).*
    target_label: __name__
    replacement: ${1}
```

リラベル後の名前でのクエリ例:

```promql
# rejections per consumer
sum by (ai_consumer) (rate(gpustack_rate_limit_rejected_total[5m]))

# which combination and window produce them
sum by (rl_combo, rl_kind, rl_period) (rate(gpustack_rate_limit_rejected_total[5m]))

# enforcement sanity: rejected share of checked requests
sum (rate(gpustack_rate_limit_request_total{rl_result="limited"}[5m]))
/
sum (rate(gpustack_rate_limit_request_total[5m]))
```

!!! note

    Higress 2.1.x で AI statistics プラグインを有効にしている場合、生のエクスポジションには素の `gpustack_rate_limit_*` 名は存在しません — 上記のリラベリングの後にのみ現れます。なければ、常に `route_upstream_model_consumer_metric_` プレフィックスを通してクエリしてください。各ウィンドウで現在实际にカウントされている内容については、`x-ratelimit-limited-key` が名指す Redis キーが信頼できるソースです。

## リコンサイラーのイベント { #reconciler-events }

サーバーはリコンサイラーを実行しており、APIキー、プリンシパル、メンバーシップ、設定の各テーブルを監視し、1リコンサイル間隔以内（デフォルトで30秒、`GPUSTACK_GATEWAY_RATE_LIMIT_RECONCILE_INTERVAL_SECONDS`）にレート制限の WasmPlugin CR を再公開します。イベントとログ行には `gateway_rate_limit_reconciler` のソースタグが付きます（例: `gateway_rate_limit_reconciler.api_key`）。このタグはイベントソースを示すものであり、Prometheus メトリクスではありません — リコンサイラー自体はメトリクスをエクスポートしません。イベントログでこのタグが見えるということは、上限の変更が取り込まれてゲートウェイへ反映されたことを意味します。
