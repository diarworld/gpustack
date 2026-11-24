---
translation_status: translated
---

# Hız Sınırlandırma { #rate-limiting }

GPUStack, trafiğin bir model örneğine ulaşmasından önce, ağ geçidinde kullanıcı
başına istek hızı ve token tüketimi limitlerini uygulayabilir. Limitler, Higress
ağ geçidinde çalışan `gpustack-rate-limit` eklentisi tarafından uygulanır;
sayaçlar Redis'te tutulur, böylece her ağ geçidi replikasında geçerli kalırlar.

## Nasıl Çalışır { #how-it-works }

- **İstek hızı (RPM)** — istek cinsinden sayılan kayan dakikalık pencere.
- **Token hızı (TPM)** — her isteğin tükettiği token'lar cinsinden sayılan kayan
  dakikalık pencere.
- **Aylık token bütçesi** — dağıtım saat diliminde, ayın 1'inde sıfırlanan
  takvime hizalı bir hak.
- **Haftalık token bütçesi** — 7 günlük kayan bir pencere.
- **Model başına eşzamanlı istek** — bir kullanıcının isteklerinden kaçının aynı
  anda tek bir model üzerinde çalışabileceğini sınıran üst sınır; istek
  çalıştığı sürece bir slot alınır ve akışı bittiğinde serbest bırakılır.

Bir limiti aşan istek, herhangi bir model örneğine başvurulmadan önce ağ
geçidinde OpenAI hata biçiminde `429 Too Many Requests` ile reddedilir.
Reddedilme gövdesi tetiklenen limiti adıyla belirtir —
`Too many requests per minute`, `Weekly token quota limit reached, reset in
usage overview`, `Too many concurrent requests for this model` — ve kayan ile
takvim pencereleri, pencerenin boşalmasına kalan saniyeyi taşıyan bir
`Retry-After` başlığı içerir (eşzamanlılık reddedilmeleri bunu taşımaz: bir
slot, sabit bir zamanda değil, çalışan bir istek tamamlandığında serbest
kalır). Reddedilen yanıt ayrıca reddi tetikleyen Redis anahtarının adını
taşıyan bir `x-ratelimit-limited-key` başlığı içerir; bu başlık bir 429'u onu
üreten kullanıcıya ve pencereye geri bağlar.

Limitler **kullanıcı bazında** kovalara ayrılır: bir kullanıcının API anahtar
trafiği ile arayüz trafiği aynı haktan harcar. Bir istek yalnızca çıkarım
yollarında (standart OpenAI/Anthropic uç noktaları ve `/model/proxy`) hız
sınırlandırmaya tabidir.

## Dağıtım Genelinde Varsayılanlar { #deployment-wide-defaults }

Varsayılanlar, kendilerine ait açık bir limiti olmayan her tüketici için
geçerlidir. Yeni bir dağıtım bunları sunucu yapılandırma dosyasından (sunucunun
`--config-file` veya `GPUSTACK_CONFIG_FILE` ortam değişkeni aracılığıyla
yüklediği YAML dosyası) tohumlar:

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

Bu andan itibaren varsayılanlar veritabanında yaşar ve varsayılanları geçersiz
kılan Kullanım → Limits sekmesinde (**Dağıtım varsayılanları**) düzenlenir —
yapılandırma dosyası yalnızca bir ilk tohum olarak ve formun boş bıraktığı
alanların kaynağı olarak hizmet vermeye devam eder. Formdaki boş bir alan
(veritabanında bir `null`) yapılandırma dosyasındaki değeri devralır; böylece
bir dağıtım, yeniden başlatmadan bir varsayılanı dosyaya geri taşıyabilir.

`default_concurrency` dışındaki tüm `default_*` alanları isteğe bağlıdır;
ayarlanmamış bir alan, o türde bir sınır olmadığı anlamına gelir.
`default_concurrency` varsayılan olarak **1**'dir — kullanıcı ve model başına
aynı anda tek sürmekte olan istek — çünkü düzinelerce paralel akışı elinde
tutan kontrolsüz bir toplu iş istemcisi, bu üst sınırın varlık sebebi olan
hata modudur; eşzamanlılık üst sınırını dağıtım genelinde kapatmak için `0`
yapın. Diğer her alan yapılandırılana kadar passthrough kalır. `rejected_code`
/ `rejected_msg` varsayılan olarak yoktur, böylece eklentinin limit bazındaki
reddedilme metinleri geçerli olur; açık bir değer bunları geçersiz kılar.

### Serbest Pencereler { #free-windows }

Aynı form, dağıtım genelinde bir **serbest pencere** yayımlar: günlük bir zaman
aralığı (gece yarısını aşabilir) artı tamamen serbest günler. Takvim, eklentinin
`free_windows` yapılandırmasını kullanır ve eklenti genelindeki `timezone`
içinde değerlendirilir:

```yaml
free_windows:
  enabled: true
  start: "15:00"        # HH:MM in the plugin-wide timezone
  end: "06:00"          # may wrap past midnight
  days: ["sat", "sun"]  # mon..sun; empty = the daily window every day
```

Takvim etkinken eklenti her denetimi askıya alır: orada kabul edilen istekler
hiçbir limite tabi tutulmaz, eşzamanlılık slotu almaz ve sayaçlara hiçbir şey
eklemez — **pencere içinde başlayan** bir istek, akışı sonun ötesine taşsa bile
tümüyle ücretsizdir. `ai-statistics` kaynaklı kullanım istatistikleri
etkilenmez. `gpustack_rate_limit_free_skipped_total` metriği atlanan istekleri
sayar. Arayüz takvimi tarayıcınızın saat diliminde gösterir; saklanan değerler
(ve günler) UTC duvar saatine göredir.

Redis, ağ geçidi pod'larından erişilebilir olmalıdır. Ağ geçidine hizmet veren
McpBridge içinde bildirin (ya da Higress yapılandırmasında
`global.onlyPushRouteCluster: false` ayarladıktan sonra `service_name`'i bir
Kubernetes Service'e işaret edin).

## Kullanıcı Başına Limitler { #per-user-limits }

Açık limitler dağıtım varsayılanlarını geçersiz kılar. Bunlar kullanıcı
hesabında ayarlanır — `rpm_limit`, `tpm_limit`, `weekly_token_limit` ve
`monthly_token_limit` — ve kullanıcının tüm API anahtarları ile arayüz
oturumları genelinde paylaşılır.

Haftalık bütçenin dağıtım genelinde bir varsayılanı yoktur: yalnızca açık
haftalık bütçesi olan hesaplar bu sınıra tabidir. Açık limiti olmayan
kullanıcılar, gruplarının kotalarına ve dağıtım varsayılanlarına göre yönetilir
— bkz. [Grup Kotaları](#group-quotas).

### Model Başına Eşzamanlılık { #per-model-concurrency }

`default_concurrency` dağıtım varsayılanı (dağıtım değiştirmediği sürece 1),
bir kullanıcının aynı anda **tek bir modele** karşı kaç istek
çalıştırabileceğini sınırlar: ilki hâlâ akış sürdürürken aynı modele yönelik
ikinci paralel istek 429 ile reddedilir; farklı modellere yönelik istekler ise
bağımsız ilerler. Sayılan sürmekte olan isteklerdir, bir hız değildir — slot,
istek ağ geçidini geçtiğinde alınır ve akışı bittiğinde serbest bırakılır;
böylece biten bir istek slotunu hemen boşaltır.

Yöneticiler eşzamanlılık üst sınırından muaftır. Model bazlı geçersiz kılmalar,
kullanıcı hesabında `model_concurrency_limits` aracılığıyla ayarlanır — model
adının, hesap için o modelin üst sınırına eşlendiği bir harita; `0` açıkça
sınırsız demektir:

```json
{
  "model_concurrency_limits": {
    "glm-5.3-flash": 4,
    "qwen3.8-27b": 2,
    "text-embedding-model": 0
  }
}
```

Haritada bulunmayan modeller `default_concurrency`'yi devralır. Bir yönetici
haritayı kullanıcı ayarları çekmecesinde kullanıcı bazında düzenleyebilir;
değişiklikler bir uzlaştırma aralığı içinde (varsayılan 30 saniye) geçerli
olur.

!!! note "Not"

    Serbest bırakılması kaybedilen bir slot — akış ortasında bir ağ geçidi
    çökmesi — bir saat sonra otomatik olarak geri alınır (eklenti üzerinde
    `concurrency_stale_seconds`, `gateway_rate_limit.plugin_config`
    aracılığıyla ayarlanabilir); böylece çökmüş bir akış bir kullanıcının
    slotunu sonsuza dek kilitleyemez.

Değişiklikler bir uzlaştırma aralığı içinde geçerli olur (varsayılan 30 saniye,
bkz. `GPUSTACK_GATEWAY_RATE_LIMIT_RECONCILE_INTERVAL_SECONDS`).

!!! note "Not"

    Hız limitleri ağ geçidinde uygulanır, bu yüzden yalnızca oradan gelen
    trafiğe uygulanırlar. Doğrudan sunucunun API portuna yapılan çıkarım
    çağrıları hız sınırlandırmaya tabi değildir.

## Grup Kotaları { #group-quotas }

Gruplar, kullanıcı hesaplarıyla aynı limit alanlarını taşır — RPM, TPM,
haftalık ve aylık token bütçeleri ve model başına sürmekte olan istek üst
sınırları — `Access Control` → `Groups` altında **Manage limits** ile
ayarlanır. Bir grup limiti asla grubu bir bütün olarak sınırlamaz: her üyenin
etkin limitleri şu öncelikle **alan bazında çözülür**:

1. kullanıcının açık limiti — hesapta ayarlanır, her iki yönde de kazanır;
2. aksi hâlde, kullanıcının ait olduğu gruplar arasındaki en küçük değer (iç
   içe alt gruplar dahil). Bir grup limiti, dağıtım varsayılanlarını hem
   sıkılaştırır hem gevşetir — bir ekibe herkesten daha yüksek bir RPM vermek
   meşru bir kullanımdır;
3. aksi hâlde, dağıtım genelindeki varsayılanlar.

Örneğin `default_rpm: 100` iken, biri RPM 10 ile sınırlı bir grubun, diğeri
RPM 2 ile sınırlı başka bir grubun üyesi olan, açık limiti olmayan bir kullanıcı
RPM 2'de çalışır; aynı gruplar TPM'yi 1000 ve 10000'de sınırlıyorsa TPM 1000'de
çalışır: her alan kendi başına çözülür. Hesap RPM 30'u açıkça ayarlıyorsa,
gruplar ne derse desin 30 geçerlidir. Haftalık bütçenin dağıtım genelinde
varsayılanı yoktur; bu yüzden değeri kullanıcının satırından, en küçük grup
değerinden ya da hiçbirinden gelir.

Grup düzeyindeki model bazlı üst sınırlar bir defada tek model için çözülür:
kullanıcının kendi `model_concurrency_limits` girdisi kazanır (saklanmış bir
`0` açıkça sınırsızdır); aksi hâlde üyenin grupları arasındaki en küçük sıfır
olmayan üst sınır uygulanır; o da yoksa `default_concurrency`. Bir **grupta**
`0`, "bu grup modeli kısıtlamaz" demektir — asla karşılaştırmaya girmez;
böylece dağıtım varsayılanını ne sıkılaştırabilir ne de kardeş bir grubun üst
sınırını kaldırabilir.

Çözümleme sunucuda yapılır: ağ geçidi eklentisi çözülmüş sayılarla kullanıcı
başına tek bir kombinasyon almaya devam eder ve grupları asla görmez. Bir kota
düzenlemesi ya da üyelik değişikliği (bir kullanıcının gruba katılması veya
ayrılması) bir uzlaştırma aralığı içinde (varsayılan 30 saniye) algılanır.
Kullanım sayfası ve `GET /v2/usage/rate-limits` uç noktası, çözülmüş değerleri
— fiilen uygulanan sayıları — raporlar.

## Hata Davranışı { #failure-behaviour }

Hız sınırlayıcı hata durumunda açılır (fail open): Redis'e ulaşılamadığında
istekler geçmeye devam eder ve bir uyarı günlüğe yazılır — trafik, sınırlayıcının
kendi altyapısı yüzünden engellenmez. Limitlerin uygulandığının kanıtı için
aşağıdaki Metrikler bölümündeki reddedilme sayaçlarını izleyin.

## Güncel Limitler ve Kullanım { #current-limits-and-usage }

Sunucu, ağ geçidinin şu anda neyi uyguladığını `GET /v2/usage/rate-limits`
üzerinden ortaya koyar. Kimliği doğrulanmış her kullanıcı varsayılan olarak
kendi görünümünü alır; platform genelinde bir yönetici (bir Org sabitlemeden
hareket eden yönetici — kullanım rotalarının kiracılar arası görünümlere
uyguladığı kapının aynısı) herhangi bir kullanıcıyı görüntülemek için
`user_id` geçebilir; diğer herkes sessizce kendi görünümünü alır.

Yanıt şunları taşır:

- `mode` — `redis` (canlı sayaçlar, istek anında Redis'ten okunur), `local`
  (Redis olmadan hız sınırlandırma açıktır, bu yüzden limitler sayaçsız döner)
  ya da `disabled` (özellik kapalıdır; Kullanım sayfası bölümü gizler).
- `user` — kullanıcının haftalık ve aylık token bütçeleri (hangileri ayarlıysa),
  her biri canlı penceresiyle, artı `model_concurrency` — hesapta ayarlanmış
  model başına sürmekte olan istek üst sınırları (`model_concurrency_limits`),
  her biri canlı `running` slot sayısıyla. Yalnızca açıkça adı geçen modeller
  tek tek listelenir; yöneticiler (muaf) ve geçersiz kırması olmayan
  kullanıcılar boş bir liste alır. `concurrency_default`, diğer modellerin
  devraldığı dağıtım genelindeki üst sınırı taşır (üst sınır kapalıyken
  `null`).
- `generated_at` — her pencerenin okunduğu an.
- `usage_error` — yalnızca Redis okuması başarısız olduğunda ayarlanır ve
  yalnızca özel durum sınıfının adını taşır, asla mesajını taşımaz (mesaj ana
  makine adları içerebilir).

Değeri olmayan alanlar `null` olarak gönderilmek yerine atlanır: `local`
modunda ya da Redis'e ulaşılamadığında bir pencerenin basitçe `used`'ı olmaz.
Okuma hata durumunda açılır — limitler her zaman döndürülür. Kullanım
sayfasının Summary sekmesi bu yanıtı **Rate limits** bölümü olarak görüntüler.

Varsayılanları 3 RPM, 768000 TPM ve aylık 100000000 token olan bir dağıtımda,
haftalık bütçesi 100000 olan bir kullanıcı için:

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

Her pencere kullanıcı bazında kovalara ayrılır: API anahtar trafiği ile arayüz
trafiği aynı haktan harcar, dolayısıyla ikisi de aynı sayacı okur. Takvim
pencerelerinin `window_seconds`'ı yoktur — uzunlukları takvimi izler ve
`reset_seconds` değerleri, dağıtım saat dilimindeki sınıra geriye doğru sayar.

### Sayaç Kurtarma { #counter-recovery }

Eklentinin Redis sayaçları tek uygulama durumudur: hız sınırlandırma Redis'i
bunları kaybettiğinde — yeni bir örnek, bir yük devretme, kalıcılığı olmayan
bir temizleme (flush) — takvim kotaları sıfırdan yeniden başlar ve gün /
hafta / ay bütçesini çoktan harcamış kullanıcılar, pencerenin kalanı için bu
bütçeye geri kavuşur. Bu boşluğu kapatmak için sunucu, `model_usage_details`
içine çoktan kaydettiği tüketimi, eklentinin okuduğu sayaçların kendilerine
yeniden oynatır; hem de yalnızca henüz var olmayan sayaçlara — canlı bir
sayaca asla dokunulmaz. Bu tur bir döngüyle çalışır (5 dakikada bir): her
döngü sayaçları tek bir ardışık (pipelined) EXISTS ile yoklar ve ayrıntı
taraması yalnızca gerçekten eksik bir şey olduğunda çalışır; böylece bir Redis
temizlemesi tek bir aralık içinde kendini onarır.

Tasarım gereği tam değildir:

- Dakika pencereleri yeniden inşa edilmez — 60 saniyede yaşlanıp düşerler, bir
  yeniden inşanın önemi olabilmesinden çok önce.
- Yeniden inşa edilen bileşenler saatlik özetlerdir; bu yüzden kayan hafta,
  istek başına değil saat adımlarıyla erir.
- Eklenti ağ geçidinde sayarken sunucu proxy tarafında kaydettiği için, yarıda
  kesilen ya da izlenmeyen istekler, yeniden inşa edilen toplamı kaybolenden
  hafifçe saptırabilir.

Hız sınırlandırma Redis'inde kalıcılığı (AOF / RDB) etkinleştirmek, yeniden
inşayı tümüyle ortadan kaldırır.

!!! note "Not"

    Uç nokta ve yeniden inşa eden bileşen, ağ geçidinin sayaçlarını yazdığı
    aynı Redis ile konuşur — `gateway_rate_limit.redis` — sunucunun üst
    düzeydeki `redis_url`'i ile değil.

### Bütçeyi Sıfırlama { #resetting-a-budget }

Kullanım sayfasındaki haftalık ve aylık bütçe kartları bir sıfırlama düğmesi
taşır: düğmeye basmak, kullanıcının o pencere türündeki sayaçlarını siler; böylece
bütçe, doğal sınırı beklemek yerine pencerenin ortasında sıfırdan yeniden
başlar. Her basış `gateway_rate_limit_resets` içine kaydedilir — gerekçe notu
zorunludur ve satıra, kullanıcıyla, basış tarihiyle ve bir yönetici birinin
adına bastıysa kimin bastığıyla birlikte düşer.

Pencere başına iki basış ücretsizdir; kota, basışlarla birlikte "yüzer" — tek
bir hafta içinde yapılan iki sıfırlama, bir sonrakini bir sonraki takvim
sınırında değil, ilk basıştan bir hafta sonra kullanılabilir kılar. İkisi de
harcandığında, en eski basış pencereden çıkana kadar düğme devre dışı kalır.
Platform genelinde bir yönetici kota ile sınırlı değildir: onların basışları
(kendi bütçesi ya da başka bir kullanıcısı) kaydedilir ama asla kullanıcının
kendi hakkını harcamaz.

Sayaç yeniden inşası sıfırlamalarla iş birliği yapar: bir pencere türündeki en
son sıfırlamadan önce kaydedilmiş kullanım affedilir; böylece bir Redis
kaybından sonraki yeniden inşa, affedilen harcamayı diriltmek yerine
sıfırlamadan kaldığı yerden devam eder.

Serbest pencere takvimine de aynı şekilde uyar: eklenti, pencere içindeki
istekleri saymadan kabul eder; bu yüzden takvim yürürlüğe girdikten sonra
(ayarlar satırının oluşturulmasından sonra) kaydedilen ve isteği pencere içinde
başlayan satırlar yeniden oynatmanın dışında tutulur — aksi hâlde her sunucu
yeniden başlatması serbest zamanı bütçelere geri fatura ederdi. Devre dışı bir
takvim hiçbir şeyi dışında tutmaz.

## Metrikler { #metrics }

Eklenti, hız sınırlandırma kararı başına üç sayaç bildirir:

- `gpustack_rate_limit_request_total` — limit denetimlerinden geçen istekler;
  `result="passed"` ve `result="limited"` olarak ayrılır.
- `gpustack_rate_limit_rejected_total` — reddedilen istekler; reddi tetikleyen
  kombinasyona ve pencereye atfedilir.
- `gpustack_rate_limit_value_total` — her pencere kovasına yazılan harcama
  (sorgu pencereleri ve eşzamanlılık edinimleri için istek başına 1, token
  pencereleri için token sayısı).

Higress AI statistics eklentisi bunları rota düzeyindeki Envoy metrikleriyle
birleştirir; bu yüzden ağ geçidinin Prometheus uç noktasında
`route_upstream_model_consumer_metric_` öneki altında ortaya çıkarlar ve
kombinasyon ile pencere adın içine gömülürken yapay zekâ boyutları etiket
olarak eklenir:

```promql
route_upstream_model_consumer_metric_gpustack_rate_limit_request_total_rule_gpustack_rate_limit_result_passed{ai_route="ai-route-route-1.internal",ai_cluster="outbound_80__model-1-46.static",ai_model="glm-5.3-flash",ai_consumer="a3d73e9f0c402120.gpustack-6"}
```

- `ai_consumer` — kimliği doğrulanmış tüketici, örn. `a3d73e9f0c402120.gpustack-6`.
- `ai_route`, `ai_cluster`, `ai_model` — isteğin yöneldiği yapay zekâ rotası ve
  model örneği.
- Kombinasyon ve pencere, ad parçalarına dönüşür (alfanümerik olmayanlar `_`
  olarak düzleşir):
  `..._rejected_total_rule_gpustack_rate_limit_combo_<combination>_kind_<query|token_rolling|token_calendar>_period_<60s|604800s|each_day|each_month>_bucket_consumer_<consumer>`.

Sayaçlar, ağ geçidi pod'ları tarafından `:15020/stats/prometheus` üzerinde
dışa aktarılır; pod'lar standart `prometheus.io/scrape: "true"`,
`prometheus.io/port: "15020"` ve `prometheus.io/path: "/stats/prometheus"`
ek açıklamalarını taşır, böylece pod ek açıklamalarını toplayan bir Prometheus
bunları yakalar.

### Yeniden Etiketlemeyle İsimleri Kısaltma { #shortening-the-names-with-relabeling }

Envoy istatistiklerinin etiket boyutları yoktur — eklenti bunları yalnızca
istatistik adının içine kodlayabilir ve `ai_*` etiketlerini üreten şey,
Higress'in yerleşik başlatma (bootstrap) çıkarıcılarını çalıştı yapan yapay
zekâ iskeleti önekidir. Küme tarafındaki çıkarıcı yapılandırması bu Higress
sürümünde sabittir (bir EnvoyFilter BOOTSTRAP yaması dikkate alınmaz); bu
yüzden kimlikleri etiket olarak taşıyan kısa isimlere ulaşmanın yolu,
Prometheus tarafında `metric_relabel_configs` kullanmaktır. Ağ geçidi toplama
işine (scrape job) uygulandığında bu, yukarıdaki isimleri
`rl_combo` / `rl_kind` / `rl_period` / `rl_bucket` / `rl_result` etiketleriyle
`gpustack_rate_limit_{request,rejected,value}_total` biçimine dönüştürür:

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

Yeniden etiketlenmiş isimler cinsinden örnek sorgular:

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

!!! note "Not"

    AI statistics eklentisinin etkin olduğu Higress 2.1.x'te çıplak
    `gpustack_rate_limit_*` isimleri ham çıktıda bulunmaz — yalnızca yukarıdaki
    yeniden etiketlemeden sonra görünürler. Onsuz, her zaman
    `route_upstream_model_consumer_metric_` öneki üzerinden sorgulayın. Her
    pencerede o anda neyin sayıldığı için, `x-ratelimit-limited-key`'in adını
    verdiği Redis anahtarları doğruluk kaynağıdır.

## Uzlaştırıcı Olayları { #reconciler-events }

Sunucu, API anahtarı, prensip (principal), üyelik ve ayarlar tablolarını izleyen
ve hız sınırlandırma WasmPlugin CR'sini bir uzlaştırma aralığı içinde (varsayılan
30 saniye, `GPUSTACK_GATEWAY_RATE_LIMIT_RECONCILE_INTERVAL_SECONDS`) yeniden
yayımlayan bir uzlaştırıcı çalıştırır. Olayları ve günlük satırları
`gateway_rate_limit_reconciler` kaynak etiketini taşır (örn.
`gateway_rate_limit_reconciler.api_key`). Bu etiket bir olay kaynağını
işaretler, Prometheus metriğini değil — uzlaştırıcının kendisi hiçbir metrik
dışa aktarmaz. Olay günlüğünde onu görmek, bir limit değişikliğinin algılanıp
ağ geçidine itildiği anlamına gelir.
