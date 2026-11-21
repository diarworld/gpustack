---
translation_status: translated
---

# Kotalar { #quotas }

Kullanım → Genel Bakış sayfasındaki **Limits** bölümü, ağ geçidinin trafiğinize şu anda uyguladığı kotaları gösterir: her hakkın ne kadarının kullanıldığını, her pencerenin ne zaman sıfırlandığını ve haftalık ile aylık bütçeler için bir sıfırlama düğmesini. Bölüm her zaman yalnızca size aittir: kendi limitlerinizi görürsünüz, bir yönetici kendi limitlerini görür.

![Usage quotas](../assets/usage/usage-quotas.png)

## Limit Kartları { #the-limit-cards }

Her kartta kullanılan / limit değeri, bir ilerleme çubuğu ve sıfırlama geri sayımı bulunur. Kart üzerindeki `?` simgesi pencereyi açıklar.

| Kart      | Pencere                                                                                    | Sıfırlama                                        |
| --------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------ |
| **RPM**   | Dakika başına istek — kayan 60 saniyelik pencere.                                           | En eski istekler pencereden çıkarken sürekli.     |
| **TPM**   | Dakika başına token — aynı kayan pencere, token cinsinden.                                  | Sürekli.                                         |
| **Hafta** | 7 günlük kayan penceredeki token'lar — tüm API anahtarlarınız ve arayüz oturumlarınız tarafından paylaşılır. | Bir haftalık kullanım pencereden çıkarken sürekli. |
| **Ay**    | Takvim ayındaki token'lar.                                                                  | Dağıtım saat diliminde ay sınırında.              |

Limite ulaşıldığında, sonraki istekler herhangi bir model örneğine ulaşmadan önce ağ geçidinde `429 Too Many Requests` ile reddedilir — pencerede yer açılana kadar.

RPM ve TPM kısa patlamalara karşı korur; haftalık ve aylık bütçeler sürekli tüketimi sınırlar. Bir istek, dokunabileceği her pencereye sayılır: API anahtar trafiğiniz ve arayüz oturumlarınız aynı kullanıcı düzeyi hakkı kullanır.

## Anahtar Başına Günlük Kullanım { #per-key-daily-usage }

Kartların altındaki tablo, API anahtarlarınızın her birini günlük token hakkına karşı gösterir — anahtarın kendi günlük limiti veya dağıtım varsayılanı — gün için kullanılan / limit değeriyle birlikte. Günlük pencere takvime hizalıdır ve dağıtım saat diliminde gece yarısı sıfırlanır.

## Model Başına Eşzamanlı İstek { #model-concurrency }

Hız ve bütçe pencerelerinin ötesinde, ağ geçidi isteklerinizin — tüm API anahtarlarınız ve oturumlarınız genelinde — tek bir model üzerinde aynı anda çalışabileceği sayıyı sınırlar. Dağıtım varsayılanı **1**'dir: bir modele yönelik bir istek hâlâ çalışırken, aynı modele yönelik ikinci paralel istek, ilki bitene kadar `429 Too Many Requests` ile reddedilir. Slot, istek tamamlanır tamamlanmaz serbest kalır — pencere beklenmez.

Her model ayrı sayılır: bir modeldeki devam eden istek, diğer modellere yapılan istekleri engellemez. Yöneticiler bu sınıra tabi değildir.

Varsayılan ve model başına geçersiz kılmalar, yönetici tarafından hesabınızda ayarlanır. Açık bir geçersiz kılması olmayan model dağıtım varsayılanını izler; `0` geçersiz kırması o model için sınırı tamamen kaldırır.

## Sayılar Nereden Geliyor { #where-the-numbers-come-from }

- **Dağıtım varsayılanları** — yönetici tarafından herkes için ayarlanır: RPM, TPM, günlük ve aylık token'lar. Ayarlanmamış bir varsayılan, o pencerenin sınırlandırılmadığı anlamına gelir.
- **Kullanıcı bütçeleri** — yöneticinin hesabınıza ayarladığı haftalık ve aylık token bütçeleri; tüm anahtarlarınız ve oturumlarınız tarafından paylaşılır.
- **Anahtar limitleri** — tek bir anahtar üzerinde RPM, TPM ve günlük token limiti. Ayarlanmamış alanlar dağıtım varsayılanlarını devralır.

Bu dağıtım şu varsayılanları kullanır:

| Pencere   | Varsayılan                                                              |
| --------- | ----------------------------------------------------------------------- |
| RPM       | dakikada 10 istek                                                        |
| TPM       | dakikada 768.000 token                                                  |
| Gün       | API anahtarı başına günlük 10.000.000 token                             |
| Ay        | kullanıcı başına aylık 100.000.000 token                                |
| Hafta     | varsayılan yok — yalnızca açık haftalık bütçesi olan hesaplar sınırlanır |

 Dahili detaylar — ağ geçidi yapılandırması, varsayılanlar, hata davranışı ve metrikler — [Rate Limiting](rate-limiting.md) bölümünde açıklanmıştır.

## Bütçeyi Sıfırlama { #resetting-a-budget }

**Hafta** ve **Ay** kartlarında bir **Reset** düğmesi bulunur. Düğmeye basmak pencerenin sayacını anında sıfırlar: bütçe doğal sınırı beklemeden yeniden başlar. Zaten kaydedilmiş kullanım kullanım raporlarında kalır — sıfırlama yalnızca kota sayacını etkiler.

Sıfırlama bir gerekçe gerektirir. Not, basışla birlikte kaydedilir — kim, ne zaman ve neden — böylece denetim kaydı her bütçenin neden yeniden başladığını gösterir.

!!! note

    Sıfırlama yalnızca haftalık ve aylık bütçeler için geçerlidir. RPM, TPM ve anahtar başına günlük kotalar kendi kendine boşalan kısa pencerelerdir.

### Sıfırlama Kotası { #reset-quota }

Pencere başına iki sıfırlama ücretsizdir. Kota, basışlarla birlikte "yüzer": bir hafta içinde yapılan iki sıfırlama, bir sonrakini bir sonraki takvim sınırında değil, ilk basıştan bir hafta sonra kullanılabilir kılar. İkisi de harcandığında, en eski basış pencereden çıkana kadar düğme devre dışı kalır — devre dışı düğmenin üzerine gelindiğinde bir sonraki sıfırlamanın ne zaman mümkün olacağı gösterilir.

Yöneticiler kota ile sınırlı değildir: bir yönetici bir bütçeyi — kendi veya başka bir kullanıcının — her zaman sıfırlayabilir ve yöneticinin basışları kullanıcının kendi iki ücretsiz sıfırlamasını harcamaz.
