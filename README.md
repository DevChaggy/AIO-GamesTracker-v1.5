# GamerPower Telegram VIP by DevChaggy0x1

نسخة VIP مطورة على Telegram فقط.

## المزايا VIP
- إشعارات فورية لكل Giveaway جديد
- زر مباشر لفتح العرض
- زر تسجيل الاستفادة
- تنبيه قرب انتهاء العرض
- فلترة لكل مستخدم حسب:
  - المنصة
  - النوع
  - الحد الأدنى للقيمة
  - Active فقط
- وضع Digest only للمستخدم
- لوحة VIP شخصية
- لوحة مالك/أدمن متقدمة
- إضافة الشات الحالي للبث أو حذفه
- إدارة حجم Digest ونافذة القرب
- تشغيل كامل عبر GitHub Actions فقط

## الأوامر
### المستخدم
- /start
- /stop
- /help
- /me
- /top
- /panel
- /setname اسمك
- /platform steam,epic games
- /type game,loot,beta
- /minworth 10
- /digestonly on
- /muteexpiring on
- /resetprefs

### المالك/الأدمن
- /owner
- /addadmin 123456789
- /removeadmin 123456789

## Secrets المطلوبة
- TELEGRAM_BOT_TOKEN
- OWNER_TELEGRAM_ID

## ملاحظات دقيقة
- تتبع ضغط زر URL الخارجي غير ممكن عبر Telegram فقط.
- لا يمكن معرفة من أخذ اللعبة فعليًا من المنصة عبر GamerPower API.
- الإحصاءات تعتمد على تفاعل المستخدم داخل البوت.
- GitHub Actions المجدول ليس لحظيًا 100% وقد يتأخر أحيانًا قليلًا.

## المصدر
Data powered by GamerPower API
https://www.gamerpower.com/api-read
