# Upload images
所需的套件 
```python
from pymongo import MongoClient
from PIL import Image
import io
import os
```
須注意變數
```python
images_collection #是DB的照片Collection，funtion裡面會用到
```
包含兩個function，一個是上傳一張照片，一個是上傳一個資料夾裡所有的照片
```python
def upload_one_new_image(img_path, up_loader, tags_list_like=[], description="null")
def upload_file_of_new_images(img_file_path, up_loader, tags_list_like=[], description="null")
```
* img_path : string 圖片路徑
* up_loader : string 誰上傳的或是專案名稱
* tags_list_like : list of string 給他的tag 
* description : string 對他的其他描述

```python
upload_one_new_image("./images/5.jpg", "Andy", ["room", "white"], "my restroom")
upload_file_of_new_images("./images2", "Andy", ["andy", "young"], "myself but younger")
```