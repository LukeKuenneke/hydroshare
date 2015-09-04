import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'hydroshare.settings'

import django
from django.core import serializers
from hs_tools_resource.models import ToolResource 

django.setup()
data = serializers.serialize("json", ToolResource.objects.all(), fields=('short_id', 'public', 'comments_count', 'rating_count', 'rating_average', 'rating_sum', 'user', 'creator', 'owners', 'view_users', 'edit_users'))
out = open("toolresource.json", "w")
out.write(data)
out.close()
