#coding:utf-8
from django.db import models

from .model import BaseModel

from datetime import datetime



class yonghu(BaseModel):
    __doc__ = u'''yonghu'''
    __tablename__ = 'yonghu'

    __loginUser__='yonghuzhanghao'


    __authTables__={}
    __authPeople__='是'#用户表，表属性loginUserColumn对应的值就是用户名字段，mima就是密码字段
    __loginUserColumn__='yonghuzhanghao'#用户表，表属性loginUserColumn对应的值就是用户名字段，mima就是密码字段
    __sfsh__='否'#表sfsh(是否审核，”是”或”否”)字段和sfhf(审核回复)字段，后台列表(page)的操作中要多一个”审核”按钮，点击”审核”弹出一个页面，包含”是否审核”和”审核回复”，点击确定调用update接口，修改sfsh和sfhf两个字段。
    __authSeparate__='否'#后台列表权限
    __thumbsUp__='否'#表属性thumbsUp[是/否]，新增thumbsupnum赞和crazilynum踩字段
    __intelRecom__='否'#智能推荐功能(表属性：[intelRecom（是/否）],新增clicktime[前端不显示该字段]字段（调用info/detail接口的时候更新），按clicktime排序查询)
    __browseClick__='否'#表属性[browseClick:是/否]，点击字段（clicknum），调用info/detail接口的时候后端自动+1）、投票功能（表属性[vote:是/否]，投票字段（votenum）,调用vote接口后端votenum+1
    __foreEndListAuth__='否'#前台列表权限foreEndListAuth[是/否]；当foreEndListAuth=是，刷的表新增用户字段userid，前台list列表接口仅能查看自己的记录和add接口后台赋值userid的值
    __foreEndList__='否'#表属性[foreEndList]前台list:和后台默认的list列表页相似,只是摆在前台,否:指没有此页,是:表示有此页(不需要登陆即可查看),前要登:表示有此页且需要登陆后才能查看
    __isAdmin__='否'#表属性isAdmin=”是”,刷出来的用户表也是管理员，即page和list可以查看所有人的考试记录(同时应用于其他表)
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    yonghuzhanghao=models.CharField ( max_length=255,null=False,unique=True, verbose_name='用户账号' )
    yonghuxingming=models.CharField ( max_length=255,null=False, unique=False, verbose_name='用户姓名' )
    mima=models.CharField ( max_length=255,null=False, unique=False, verbose_name='密码' )
    shoujihaoma=models.CharField ( max_length=255, null=True, unique=False, verbose_name='手机号码' )
    xingbie=models.CharField ( max_length=255, null=True, unique=False, verbose_name='性别' )
    nianling=models.IntegerField  (  null=True, unique=False, verbose_name='年龄' )
    touxiang=models.TextField   (  null=True, unique=False, verbose_name='头像' )
    '''
    yonghuzhanghao=VARCHAR
    yonghuxingming=VARCHAR
    mima=VARCHAR
    shoujihaoma=VARCHAR
    xingbie=VARCHAR
    nianling=Integer
    touxiang=Text
    '''
    class Meta:
        db_table = 'yonghu'
        verbose_name = verbose_name_plural = '用户'
class qichexinxi(BaseModel):
    __doc__ = u'''qichexinxi'''
    __tablename__ = 'qichexinxi'



    __authTables__={}
    __authPeople__='否'#用户表，表属性loginUserColumn对应的值就是用户名字段，mima就是密码字段
    __sfsh__='否'#表sfsh(是否审核，”是”或”否”)字段和sfhf(审核回复)字段，后台列表(page)的操作中要多一个”审核”按钮，点击”审核”弹出一个页面，包含”是否审核”和”审核回复”，点击确定调用update接口，修改sfsh和sfhf两个字段。
    __authSeparate__='否'#后台列表权限
    __thumbsUp__='否'#表属性thumbsUp[是/否]，新增thumbsupnum赞和crazilynum踩字段
    __intelRecom__='用协'#智能推荐功能(表属性：[intelRecom（是/否）],新增clicktime[前端不显示该字段]字段（调用info/detail接口的时候更新），按clicktime排序查询)
    __browseClick__='是'#表属性[browseClick:是/否]，点击字段（clicknum），调用info/detail接口的时候后端自动+1）、投票功能（表属性[vote:是/否]，投票字段（votenum）,调用vote接口后端votenum+1
    __foreEndListAuth__='否'#前台列表权限foreEndListAuth[是/否]；当foreEndListAuth=是，刷的表新增用户字段userid，前台list列表接口仅能查看自己的记录和add接口后台赋值userid的值
    __foreEndList__='是'#表属性[foreEndList]前台list:和后台默认的list列表页相似,只是摆在前台,否:指没有此页,是:表示有此页(不需要登陆即可查看),前要登:表示有此页且需要登陆后才能查看
    __isAdmin__='否'#表属性isAdmin=”是”,刷出来的用户表也是管理员，即page和list可以查看所有人的考试记录(同时应用于其他表)
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    cheming=models.CharField ( max_length=255, null=True, unique=False, verbose_name='车名' )
    chetu=models.TextField   (  null=True, unique=False, verbose_name='车图' )
    xiangqinglianjie=models.TextField   (  null=True, unique=False, verbose_name='详情链接' )
    chebiaoqian=models.CharField ( max_length=255, null=True, unique=False, verbose_name='车标签' )
    zongbiaoqian=models.CharField ( max_length=255, null=True, unique=False, verbose_name='总标签' )
    jiage=models.CharField ( max_length=255, null=True, unique=False, verbose_name='价格' )
    kuaichong=models.CharField ( max_length=255, null=True, unique=False, verbose_name='快充' )
    baigonglijiasu=models.CharField ( max_length=255, null=True, unique=False, verbose_name='百公里加速' )
    zhengcheOTA=models.CharField ( max_length=255, null=True, unique=False, verbose_name='整车OTA' )
    dianchirongliang=models.CharField ( max_length=255, null=True, unique=False, verbose_name='电池容量' )
    zidongjiashi=models.CharField ( max_length=255, null=True, unique=False, verbose_name='自动驾驶' )
    xuhang=models.CharField ( max_length=255, null=True, unique=False, verbose_name='续航' )
    clicktime=models.DateTimeField  (auto_now=True,  null=True, unique=False, verbose_name='最近点击时间' )
    clicknum=models.IntegerField  (  null=True, unique=False,default='0', verbose_name='点击次数' )
    storeupnum=models.IntegerField  (  null=True, unique=False,default='0', verbose_name='收藏数' )
    '''
    cheming=VARCHAR
    chetu=Text
    xiangqinglianjie=Text
    chebiaoqian=VARCHAR
    zongbiaoqian=VARCHAR
    jiage=VARCHAR
    kuaichong=VARCHAR
    baigonglijiasu=VARCHAR
    zhengcheOTA=VARCHAR
    dianchirongliang=VARCHAR
    zidongjiashi=VARCHAR
    xuhang=VARCHAR
    clicktime=DateTime
    clicknum=Integer
    storeupnum=Integer
    '''
    class Meta:
        db_table = 'qichexinxi'
        verbose_name = verbose_name_plural = '汽车信息'
class qichepinglun(BaseModel):
    __doc__ = u'''qichepinglun'''
    __tablename__ = 'qichepinglun'



    __authTables__={}
    __authPeople__='否'#用户表，表属性loginUserColumn对应的值就是用户名字段，mima就是密码字段
    __sfsh__='否'#表sfsh(是否审核，”是”或”否”)字段和sfhf(审核回复)字段，后台列表(page)的操作中要多一个”审核”按钮，点击”审核”弹出一个页面，包含”是否审核”和”审核回复”，点击确定调用update接口，修改sfsh和sfhf两个字段。
    __authSeparate__='否'#后台列表权限
    __thumbsUp__='否'#表属性thumbsUp[是/否]，新增thumbsupnum赞和crazilynum踩字段
    __intelRecom__='否'#智能推荐功能(表属性：[intelRecom（是/否）],新增clicktime[前端不显示该字段]字段（调用info/detail接口的时候更新），按clicktime排序查询)
    __browseClick__='否'#表属性[browseClick:是/否]，点击字段（clicknum），调用info/detail接口的时候后端自动+1）、投票功能（表属性[vote:是/否]，投票字段（votenum）,调用vote接口后端votenum+1
    __foreEndListAuth__='否'#前台列表权限foreEndListAuth[是/否]；当foreEndListAuth=是，刷的表新增用户字段userid，前台list列表接口仅能查看自己的记录和add接口后台赋值userid的值
    __foreEndList__='是'#表属性[foreEndList]前台list:和后台默认的list列表页相似,只是摆在前台,否:指没有此页,是:表示有此页(不需要登陆即可查看),前要登:表示有此页且需要登陆后才能查看
    __isAdmin__='否'#表属性isAdmin=”是”,刷出来的用户表也是管理员，即page和list可以查看所有人的考试记录(同时应用于其他表)
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    biaoti=models.CharField ( max_length=255, null=True, unique=False, verbose_name='标题' )
    pinglunneirong=models.TextField   (  null=True, unique=False, verbose_name='评论内容' )
    tupianlianjie=models.TextField   (  null=True, unique=False, verbose_name='图片链接' )
    dianzanshu=models.IntegerField  (  null=True, unique=False, verbose_name='点赞数' )
    pinglunzizhang=models.IntegerField  (  null=True, unique=False, verbose_name='评论字长' )
    pinglunrenmingcheng=models.CharField ( max_length=255, null=True, unique=False, verbose_name='评论人名称' )
    pinglunlianjie=models.TextField   (  null=True, unique=False, verbose_name='评论链接' )
    storeupnum=models.IntegerField  (  null=True, unique=False,default='0', verbose_name='收藏数' )
    '''
    biaoti=VARCHAR
    pinglunneirong=Text
    tupianlianjie=Text
    dianzanshu=Integer
    pinglunzizhang=Integer
    pinglunrenmingcheng=VARCHAR
    pinglunlianjie=Text
    storeupnum=Integer
    '''
    class Meta:
        db_table = 'qichepinglun'
        verbose_name = verbose_name_plural = '汽车评论'
class qichepeizhi(BaseModel):
    __doc__ = u'''qichepeizhi'''
    __tablename__ = 'qichepeizhi'



    __authTables__={}
    __authPeople__='否'#用户表，表属性loginUserColumn对应的值就是用户名字段，mima就是密码字段
    __sfsh__='否'#表sfsh(是否审核，”是”或”否”)字段和sfhf(审核回复)字段，后台列表(page)的操作中要多一个”审核”按钮，点击”审核”弹出一个页面，包含”是否审核”和”审核回复”，点击确定调用update接口，修改sfsh和sfhf两个字段。
    __authSeparate__='否'#后台列表权限
    __thumbsUp__='否'#表属性thumbsUp[是/否]，新增thumbsupnum赞和crazilynum踩字段
    __intelRecom__='否'#智能推荐功能(表属性：[intelRecom（是/否）],新增clicktime[前端不显示该字段]字段（调用info/detail接口的时候更新），按clicktime排序查询)
    __browseClick__='否'#表属性[browseClick:是/否]，点击字段（clicknum），调用info/detail接口的时候后端自动+1）、投票功能（表属性[vote:是/否]，投票字段（votenum）,调用vote接口后端votenum+1
    __foreEndListAuth__='否'#前台列表权限foreEndListAuth[是/否]；当foreEndListAuth=是，刷的表新增用户字段userid，前台list列表接口仅能查看自己的记录和add接口后台赋值userid的值
    __foreEndList__='否'#表属性[foreEndList]前台list:和后台默认的list列表页相似,只是摆在前台,否:指没有此页,是:表示有此页(不需要登陆即可查看),前要登:表示有此页且需要登陆后才能查看
    __isAdmin__='否'#表属性isAdmin=”是”,刷出来的用户表也是管理员，即page和list可以查看所有人的考试记录(同时应用于其他表)
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    chexi=models.CharField ( max_length=255, null=True, unique=False, verbose_name='车系' )
    xuhang=models.CharField ( max_length=255, null=True, unique=False, verbose_name='续航' )
    dianchirongliang=models.CharField ( max_length=255, null=True, unique=False, verbose_name='电池容量' )
    jiage=models.CharField ( max_length=255, null=True, unique=False, verbose_name='价格' )
    chepeizhi=models.CharField ( max_length=255, null=True, unique=False, verbose_name='车配置' )
    biaoqian=models.CharField ( max_length=255, null=True, unique=False, verbose_name='标签' )
    cheid=models.CharField ( max_length=255, null=True, unique=False, verbose_name='车id' )
    '''
    chexi=VARCHAR
    xuhang=VARCHAR
    dianchirongliang=VARCHAR
    jiage=VARCHAR
    chepeizhi=VARCHAR
    biaoqian=VARCHAR
    cheid=VARCHAR
    '''
    class Meta:
        db_table = 'qichepeizhi'
        verbose_name = verbose_name_plural = '汽车配置'
class forum(BaseModel):
    __doc__ = u'''forum'''
    __tablename__ = 'forum'



    __authTables__={}
    __foreEndListAuth__='是'#前台列表权限foreEndListAuth[是/否]；当foreEndListAuth=是，刷的表新增用户字段userid，前台list列表接口仅能查看自己的记录和add接口后台赋值userid的值
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    title=models.CharField ( max_length=255, null=True, unique=False, verbose_name='帖子标题' )
    content=models.TextField   ( null=False, unique=False, verbose_name='帖子内容' )
    parentid=models.BigIntegerField  (  null=True, unique=False, verbose_name='父节点id' )
    userid=models.BigIntegerField  ( null=False, unique=False, verbose_name='用户id' )
    username=models.CharField ( max_length=255, null=True, unique=False, verbose_name='用户名' )
    avatarurl=models.TextField   (  null=True, unique=False, verbose_name='头像' )
    isdone=models.CharField ( max_length=255, null=True, unique=False, verbose_name='状态' )
    istop=models.IntegerField  (  null=True, unique=False,default='0', verbose_name='是否置顶' )
    toptime=models.DateTimeField  (  null=True, unique=False, verbose_name='置顶时间' )
    typename=models.CharField ( max_length=255, null=True, unique=False, verbose_name='分类名称' )
    cover=models.TextField   (  null=True, unique=False, verbose_name='封面' )
    isanon=models.IntegerField  (  null=True, unique=False,default='0', verbose_name='是否匿名(1:是,0:否)' )
    delflag=models.IntegerField  (  null=True, unique=False,default='0', verbose_name='是否删除(1:是,0:否)' )
    '''
    title=VARCHAR
    content=Text
    parentid=BigInteger
    userid=BigInteger
    username=VARCHAR
    avatarurl=Text
    isdone=VARCHAR
    istop=Integer
    toptime=DateTime
    typename=VARCHAR
    cover=Text
    isanon=Integer
    delflag=Integer
    '''
    class Meta:
        db_table = 'forum'
        verbose_name = verbose_name_plural = '交流互动'
class forumtype(BaseModel):
    __doc__ = u'''forumtype'''
    __tablename__ = 'forumtype'



    __authTables__={}
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    typename=models.CharField ( max_length=255, null=True, unique=False, verbose_name='分类名称' )
    '''
    typename=VARCHAR
    '''
    class Meta:
        db_table = 'forumtype'
        verbose_name = verbose_name_plural = '交流互动类型'
class forumreport(BaseModel):
    __doc__ = u'''forumreport'''
    __tablename__ = 'forumreport'



    __authTables__={}
    __authSeparate__='是'#后台列表权限
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    forumid=models.BigIntegerField  (  null=True, unique=False, verbose_name='论坛id' )
    title=models.CharField ( max_length=255, null=True, unique=False, verbose_name='帖子标题' )
    userid=models.BigIntegerField  ( null=False, unique=False, verbose_name='举报用户id' )
    username=models.CharField ( max_length=255, null=True, unique=False, verbose_name='举报用户名' )
    reporteduserid=models.BigIntegerField  ( null=False, unique=False, verbose_name='被举报用户id' )
    reportedusername=models.CharField ( max_length=255, null=True, unique=False, verbose_name='被举报用户名' )
    reason=models.TextField   ( null=False, unique=False, verbose_name='举报原因' )
    picture=models.TextField   (  null=True, unique=False, verbose_name='图片补充' )
    handleadvise=models.TextField   (  null=True, unique=False, verbose_name='处理建议' )
    status=models.CharField ( max_length=255, null=True, unique=False,default='处理中', verbose_name='状态' )
    reporttype=models.CharField ( max_length=255, null=True, unique=False,default='主题帖举报', verbose_name='举报类型' )
    '''
    forumid=BigInteger
    title=VARCHAR
    userid=BigInteger
    username=VARCHAR
    reporteduserid=BigInteger
    reportedusername=VARCHAR
    reason=Text
    picture=Text
    handleadvise=Text
    status=VARCHAR
    reporttype=VARCHAR
    '''
    class Meta:
        db_table = 'forumreport'
        verbose_name = verbose_name_plural = '交流互动举报'
class syslog(BaseModel):
    __doc__ = u'''syslog'''
    __tablename__ = 'syslog'



    __authTables__={}
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    username=models.CharField ( max_length=255,null=False, unique=False, verbose_name='用户名' )
    operation=models.CharField ( max_length=255,null=False, unique=False, verbose_name='用户操作' )
    method=models.CharField ( max_length=255, null=True, unique=False, verbose_name='请求方法' )
    params=models.TextField   (  null=True, unique=False, verbose_name='请求参数' )
    time=models.BigIntegerField  (  null=True, unique=False, verbose_name='请求时长(毫秒)' )
    ip=models.CharField ( max_length=255, null=True, unique=False, verbose_name='IP地址' )
    '''
    username=VARCHAR
    operation=VARCHAR
    method=VARCHAR
    params=Text
    time=BigInteger
    ip=VARCHAR
    '''
    class Meta:
        db_table = 'syslog'
        verbose_name = verbose_name_plural = '系统日志'
class newstype(BaseModel):
    __doc__ = u'''newstype'''
    __tablename__ = 'newstype'



    __authTables__={}
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    typename=models.CharField ( max_length=255,null=False, unique=False, verbose_name='分类名称' )
    '''
    typename=VARCHAR
    '''
    class Meta:
        db_table = 'newstype'
        verbose_name = verbose_name_plural = '公告信息分类'
class news(BaseModel):
    __doc__ = u'''news'''
    __tablename__ = 'news'



    __authTables__={}
    __thumbsUp__='是'#表属性thumbsUp[是/否]，新增thumbsupnum赞和crazilynum踩字段
    __intelRecom__='是'#智能推荐功能(表属性：[intelRecom（是/否）],新增clicktime[前端不显示该字段]字段（调用info/detail接口的时候更新），按clicktime排序查询)
    __browseClick__='是'#表属性[browseClick:是/否]，点击字段（clicknum），调用info/detail接口的时候后端自动+1）、投票功能（表属性[vote:是/否]，投票字段（votenum）,调用vote接口后端votenum+1
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    title=models.CharField ( max_length=255,null=False, unique=False, verbose_name='标题' )
    introduction=models.TextField   (  null=True, unique=False, verbose_name='简介' )
    typename=models.CharField ( max_length=255, null=True, unique=False, verbose_name='分类名称' )
    name=models.CharField ( max_length=255, null=True, unique=False, verbose_name='发布人' )
    headportrait=models.TextField   (  null=True, unique=False, verbose_name='头像' )
    clicknum=models.IntegerField  (  null=True, unique=False,default='0', verbose_name='点击次数' )
    clicktime=models.DateTimeField  (auto_now=True,  null=True, unique=False, verbose_name='最近点击时间' )
    thumbsupnum=models.IntegerField  (  null=True, unique=False,default='0', verbose_name='赞' )
    crazilynum=models.IntegerField  (  null=True, unique=False,default='0', verbose_name='踩' )
    storeupnum=models.IntegerField  (  null=True, unique=False,default='0', verbose_name='收藏数' )
    picture=models.TextField   ( null=False, unique=False, verbose_name='图片' )
    content=models.TextField   ( null=False, unique=False, verbose_name='内容' )
    '''
    title=VARCHAR
    introduction=Text
    typename=VARCHAR
    name=VARCHAR
    headportrait=Text
    clicknum=Integer
    clicktime=DateTime
    thumbsupnum=Integer
    crazilynum=Integer
    storeupnum=Integer
    picture=Text
    content=Text
    '''
    class Meta:
        db_table = 'news'
        verbose_name = verbose_name_plural = '公告信息'
class storeup(BaseModel):
    __doc__ = u'''storeup'''
    __tablename__ = 'storeup'



    __authTables__={}
    __authSeparate__='是'#后台列表权限
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    userid=models.BigIntegerField  ( null=False, unique=False, verbose_name='用户id' )
    refid=models.BigIntegerField  (  null=True, unique=False, verbose_name='商品id' )
    tablename=models.CharField ( max_length=255, null=True, unique=False, verbose_name='表名' )
    name=models.CharField ( max_length=255,null=False, unique=False, verbose_name='名称' )
    picture=models.TextField   (  null=True, unique=False, verbose_name='图片' )
    type=models.CharField ( max_length=255, null=True, unique=False,default='1', verbose_name='类型' )
    inteltype=models.CharField ( max_length=255, null=True, unique=False, verbose_name='推荐类型' )
    remark=models.CharField ( max_length=255, null=True, unique=False, verbose_name='备注' )
    '''
    userid=BigInteger
    refid=BigInteger
    tablename=VARCHAR
    name=VARCHAR
    picture=Text
    type=VARCHAR
    inteltype=VARCHAR
    remark=VARCHAR
    '''
    class Meta:
        db_table = 'storeup'
        verbose_name = verbose_name_plural = '收藏表'
class messages(BaseModel):
    __doc__ = u'''messages'''
    __tablename__ = 'messages'



    __authTables__={}
    __hasMessage__='是'#表属性hasMessage为是，新增留言板表messages,字段content（内容），userid（用户id）
    addtime = models.DateTimeField(auto_now_add=False, verbose_name=u'创建时间')
    userid=models.BigIntegerField  ( null=False, unique=False, verbose_name='留言人id' )
    username=models.CharField ( max_length=255, null=True, unique=False, verbose_name='用户名' )
    avatarurl=models.TextField   (  null=True, unique=False, verbose_name='头像' )
    content=models.TextField   ( null=False, unique=False, verbose_name='留言内容' )
    cpicture=models.TextField   (  null=True, unique=False, verbose_name='留言图片' )
    reply=models.TextField   (  null=True, unique=False, verbose_name='回复内容' )
    rpicture=models.TextField   (  null=True, unique=False, verbose_name='回复图片' )
    '''
    userid=BigInteger
    username=VARCHAR
    avatarurl=Text
    content=Text
    cpicture=Text
    reply=Text
    rpicture=Text
    '''
    class Meta:
        db_table = 'messages'
        verbose_name = verbose_name_plural = '留言反馈'
