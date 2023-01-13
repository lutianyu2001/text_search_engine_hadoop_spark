from django.db import models

# Create your models here.


class TotalCount(models.Model):
    id = models.BigAutoField(primary_key=True)
    word = models.CharField(max_length=50)
    count = models.IntegerField()
