---
translation_status: translated
---

# Kotalar { #quotas }

Kullanım sayfasındaki **Limits** sekmesi, ağ geçidinin trafiğinize şu anda uyguladığı kotaları gösterir: her hakkın ne kadarının kullanıldığını, her pencerenin ne zaman sıfırlandığını ve haftalık ile aylık bütçeler için bir sıfırlama düğmesini. Bölüm her zaman yalnızca size aittir; bir yönetici ayrıca kartın üzerindeki seçiciden herhangi bir kullanıcıyı belirleyebilir.

![Usage quotas](../assets/usage/usage-quotas.png)

## Limit Kartları { #the-limit-cards }

Her kartta kullanılan / limit değeri, bir ilerleme çubuğu ve sıfırlama geri sayımı bulunur. Kart üzerindeki `?` simgesi pencereyi açıklar.

| Kart      | Pencere                                                                                    | Sıfırlama                                        |
| --------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------ |
| **RPM**   | Dakika başına istek — kayan 60 saniyelik pencere.                                           | En eski istekler pencereden çıkarken sürekli.     |
| **TPM**   | Dakika başına token — aynı kayan pencere, token cinsinden.                                  | Sürekli.                                         |
| **Hafta** | 7 günlük kayan penceredeki token'lar — tüm API anahtarlarınız ve arayüz oturumlarınız tarafından paylaşılır. | Bir haftalık kullanım pencereden çıkarken sürekli. |
| **Ay**    | Takvim ayındaki token'lar.                                                                  | Dağıtım saat diliminde ay sınırında.              |

Limite ulaşıldığında, sonraki istekler herhangi bir model örneğine ulaşmadan önce ağ geçidinde `429 Too Many Requests` ile reddedilir — pencerede yer açılana kadar. Reddedilme gövdesi hangi limitin tetiklendiğini adıyla belirtir (örneğin `Too many requests per minute`, `Monthly token quota limit reached, reset in usage overview` veya `Too many concurrent requests for this model`) ve kayan ile takvim pencereleri, pencerenin boşalmasına kalan saniyeyi taşıyan bir `Retry-After` başlığı içerir.

RPM ve TPM kısa patlamalara karşı korur; haftalık ve aylık bütçeler sürekli tüketimi sınırlar. Bir istek, dokunabileceği her pencereye sayılır: API anahtar trafiğiniz ve arayüz oturumlarınız aynı kullanıcı düzeyi hakkı kullanır.

## Modellere Göre Eşzamanlılık { #concurrency-by-model }

Kartların altındaki tablo, ulaşabildiğiniz hazır (ready) modelleri yoğunluk sırasına göre listeler: her model için şu an çalışan kendi isteklerinizin sayısı ve o modelin etkin limiti — ayarlanmışsa yöneticinin model bazlı geçersiz kırması, aksi hâlde dağıtım varsayılanı. Çalışma sayısı limite dayanmış bir model, `429 Too Many Concurrent Requests` reddedilmelerinin kaynağıdır.

## Model Başına Eşzamanlı İstek { #model-concurrency }

Hız ve bütçe pencerelerinin ötesinde, ağ geçidi isteklerinizin — tüm API anahtarlarınız ve oturumlarınız genelinde — tek bir model üzerinde aynı anda çalışabileceği sayıyı sınırlar. Dağıtım varsayılanı **2**'dir: bir modele yönelik iki istek hâlâ çalışırken, aynı modele yönelik üçüncü paralel istek, biri bitene kadar `429 Too Many Requests` ile reddedilir. Slot, istek tamamlanır tamamlanmaz serbest kalır — pencere beklenmez; bu yüzden bu reddedilmeler `Retry-After` başlığı taşımaz.

Her model ayrı sayılır: bir modeldeki devam eden istek, diğer modellere yapılan istekleri engellemez. Yöneticiler bu sınıra tabi değildir.

Varsayılan ve model başına geçersiz kılmalar, yönetici tarafından hesabınızda ayarlanır. Açık bir geçersiz kılması olmayan model dağıtım varsayılanını izler; `0` geçersiz kırması o model için sınırı tamamen kaldırır.

## Sayılar Nereden Geliyor { #where-the-numbers-come-from }

- **Dağıtım varsayılanları** — yönetici tarafından herkes için ayarlanır: RPM, TPM, aylık token'lar ve model başına eşzamanlılık sınırı. Ayarlanmamış bir varsayılan, o pencerenin sınırlandırılmadığı anlamına gelir.
- **Kullanıcı düzeyi limitler** — yöneticinin hesabınıza ayarladığı haftalık ve aylık token bütçeleri ile RPM ve TPM; tüm anahtarlarınız ve oturumlarınız tarafından paylaşılır.

Bu dağıtım şu varsayılanları kullanır (yöneticiler Limits sekmesinde düzenler; bkz. [Dağıtım varsayılanlarını düzenleme](#editing-the-deployment-defaults)):

| Pencere   | Varsayılan                                                              |
| --------- | ----------------------------------------------------------------------- |
| RPM       | dakikada 10 istek                                                        |
| TPM       | dakikada 768.000 token                                                  |
| Ay        | kullanıcı başına aylık 100.000.000 token                                |
| Hafta     | varsayılan yok — yalnızca açık haftalık bütçesi olan hesaplar sınırlanır |
| Eşzamanlılık | model başına 2 istek                                                  |

 Dahili detaylar — ağ geçidi yapılandırması, varsayılanlar, hata davranışı ve metrikler — [Rate Limiting](rate-limiting.md) bölümünde açıklanmıştır.

## Dağıtım Varsayılanlarını Düzenleme { #editing-the-deployment-defaults }

Yöneticiler dağıtım varsayılanlarını — RPM, TPM, aylık token bütçesi ve model başına eşzamanlılık sınırını — Kullanım → Limits sekmesinde (**Dağıtım varsayılanları**) düzenler. Boş alan "sunucu yapılandırma dosyasındaki değeri devral" demektir; eşzamanlılık için `0` sınırı tamamen kapatır. Kaydedilen değişiklik bir sonraki uzlaştırma turunda (bir dakika içinde) ağ geçidine ulaşır, yeniden başlatma gerekmez.

## Serbest Pencereler { #free-windows }

Aynı form dağıtım genelinde **serbest pencereyi** yayımlar: günlük bir zaman aralığı (gece yarısını aşabilir) artı tamamen serbest günler; değerler UTC olarak saklanır. Takvim etkin olduğunda ağ geçidi hız sınırlamayı tamamen askıya alır — her istek kabul edilir, eşzamanlılık slotu alınmaz ve limit sayaçları donar.

Pencere **içinde başlayan** bir istek, akışı pencerenin sonunu aşsa bile tümüyle ücretsizdir; dışarıda başlayan istek tamamen sınırlı kalır. Kullanım istatistikleri ve raporlar her zamanki gibi toplanmaya devam eder — duran yalnızca limit muhasebesidir.

Pencere etkin olduğunda Limits kartı, donmuş bir sayacın arıza sanılmaması için **Serbest saatler** rozeti gösterir. Form saatleri tarayıcınızın saat diliminde gösterir ve kaydederken UTC'ye çevirir; günler UTC'ye göredir.

## Bütçeyi Sıfırlama { #resetting-a-budget }

**Hafta** ve **Ay** kartlarında bir **Reset** düğmesi bulunur. Düğmeye basmak pencerenin sayacını anında sıfırlar: bütçe doğal sınırı beklemeden yeniden başlar. Zaten kaydedilmiş kullanım kullanım raporlarında kalır — sıfırlama yalnızca kota sayacını etkiler.

Sıfırlama bir gerekçe gerektirir. Not, basışla birlikte kaydedilir — kim, ne zaman ve neden — böylece denetim kaydı her bütçenin neden yeniden başladığını gösterir.

!!! note

    Sıfırlama yalnızca haftalık ve aylık bütçeler için geçerlidir. RPM, TPM ve model başına eşzamanlılık sınırı kısa ömürlüdür: istekler pencereden çıktığında ya da tamamlandığında sayaçlar kendiliğinden boşalır.

### Sıfırlama Kotası { #reset-quota }

Pencere başına iki sıfırlama ücretsizdir. Kota, basışlarla birlikte "yüzer": bir hafta içinde yapılan iki sıfırlama, bir sonrakini bir sonraki takvim sınırında değil, ilk basıştan bir hafta sonra kullanılabilir kılar. İkisi de harcandığında, en eski basış pencereden çıkana kadar düğme devre dışı kalır — devre dışı düğmenin üzerine gelindiğinde bir sonraki sıfırlamanın ne zaman mümkün olacağı gösterilir.

Yöneticiler kota ile sınırlı değildir: bir yönetici bir bütçeyi — kendi veya başka bir kullanıcının — her zaman sıfırlayabilir ve yöneticinin basışları kullanıcının kendi iki ücretsiz sıfırlamasını harcamaz.
