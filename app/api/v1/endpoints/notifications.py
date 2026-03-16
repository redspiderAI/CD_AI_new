from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import json
import pymysql

from app.database import get_db
from app.schemas.notification import NotificationQueryResponse, NotificationItem, NotificationUpdate

router = APIRouter()


class NotificationContent(BaseModel):
    title: str
    content: str


@router.post(
    "/push",
    summary="信息推送",
    description="推送通知信息，支持批量推送，记录到 user_messages 表"
)
def push_notification(
    payload: NotificationContent,
    student_ids: str | None = Query(None, description="学生ID列表（学生学号），逗号分隔，例如: 1,2,3，管理员和教师都可以使用"),
    teacher_ids: str | None = Query(None, description="教师ID列表（教师工号），逗号分隔，例如: 1001,1002，仅管理员可用"),
    current_user: str = Query(..., description="当前用户信息(JSON字符串)，示例: {\"sub\":1,\"roles\":[\"admin\"],\"username\":\"admin1\"}"),
    db: pymysql.connections.Connection = Depends(get_db),
):
    cursor = None
    try:
        # 1. 核心参数校验
        if not (student_ids or teacher_ids):
            raise HTTPException(status_code=400, detail="必须提供学生ID列表（student_ids）或教师ID列表（teacher_ids）")
        if not payload.title:
            raise HTTPException(status_code=400, detail="消息标题（title）不能为空")
        if not payload.content:
            raise HTTPException(status_code=400, detail="消息内容（content）不能为空")
        
        # 2. 解析 current_user 获取发送者信息
        try:
            import urllib.parse
            current_user = urllib.parse.unquote(current_user)
            current_user_data = json.loads(current_user)
            sender_id = str(current_user_data.get("sub"))
            sender_roles = current_user_data.get("roles", [])
            if not sender_roles:
                raise HTTPException(status_code=403, detail="无效的用户角色")
            sender_role = sender_roles[0] if sender_roles else "user"
        except Exception:
            raise HTTPException(status_code=403, detail="无效的用户信息格式")
        
        # 3. 验证发送者身份是否存在
        cursor = db.cursor()
        if "admin" in sender_roles:
            # 验证管理员是否存在
            cursor.execute("SELECT id FROM admins WHERE id = %s", (sender_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=403, detail="管理员身份不存在")
        elif "teacher" in sender_roles:
            # 验证教师是否存在
            cursor.execute("SELECT id FROM teachers WHERE id = %s", (sender_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=403, detail="教师身份不存在")
        else:
            raise HTTPException(status_code=403, detail="无权执行此操作")
        
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        
        # 4. 准备目标用户列表
        target_users = []
        
        # 处理学生ID列表
        if student_ids:
            student_id_list = [sid.strip() for sid in student_ids.split(",") if sid.strip()]
            for student_id in student_id_list:
                # 验证学生是否存在
                cursor.execute("SELECT student_id FROM students WHERE student_id = %s", (student_id,))
                if not cursor.fetchone():
                    raise HTTPException(status_code=404, detail=f"学生ID {student_id} 不存在")
                target_users.append({"user_id": student_id, "username": ""})
        
        # 处理教师ID列表
        if teacher_ids:
            # 权限验证：只有管理员可以给教师发送消息
            if "admin" not in sender_roles:
                raise HTTPException(status_code=403, detail="只有管理员可以给教师发送消息")
            teacher_id_list = [tid.strip() for tid in teacher_ids.split(",") if tid.strip()]
            for teacher_id in teacher_id_list:
                # 验证教师是否存在
                cursor.execute("SELECT teacher_id FROM teachers WHERE teacher_id = %s", (teacher_id,))
                if not cursor.fetchone():
                    raise HTTPException(status_code=404, detail=f"教师ID {teacher_id} 不存在")
                target_users.append({"user_id": teacher_id, "username": ""})
        
        # 5. 处理消息内容
        content_value = payload.content or ""
        metadata = {}
        # 如果 content 超过 TEXT 大小（防护），将超长部分保存到 metadata.long_content
        if len(content_value) > 60000:
            metadata["long_content"] = content_value[60000:]
            content_value = content_value[:60000]

        # 6. 保存 sender 信息到 metadata
        metadata["sender_id"] = sender_id
        metadata["sender_role"] = sender_role
        # 添加唯一标识符，确保每条消息都能唯一标识，避免相同消息被覆盖
        metadata["message_id"] = f"{sender_id}_{int(datetime.now().timestamp() * 1000)}"

        metadata_json = json.dumps(metadata, ensure_ascii=False) if metadata else None
        source_value = "system"  # 固定来源
        
        # 7. 组装插入SQL
        insert_sql = """
        INSERT INTO user_messages (
            user_id, username, title, content, source, status, 
            received_time, metadata, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 8. 批量执行插入操作
        inserted_ids = []
        for user in target_users:
            cursor.execute(
                insert_sql,
                (
                    user["user_id"],     # user_id（接收用户ID）
                    user["username"],    # username（接收用户名，可为空）
                    payload.title,        # title（消息标题）
                    content_value,        # content（消息内容，已按长度保护）
                    source_value,         # source（来源）
                    "unread",            # status（默认未读）
                    now_str,              # received_time（接收时间）
                    metadata_json,        # metadata（扩展元数据）
                    now_str,              # created_at（记录创建时间）
                    now_str               # updated_at（记录更新时间）
                ),
            )
            inserted_ids.append(cursor.lastrowid)
        
        db.commit()
        
        # 9. 返回推送结果
        # 构建返回的消息列表
        messages = []
        for i, user in enumerate(target_users):
            messages.append({
                "target_id": user["user_id"],
                "title": payload.title,
                "message_id": inserted_ids[i] if i < len(inserted_ids) else None
            })
        
        return {
            "message": f"消息推送成功，共推送 {len(target_users)} 条消息",
            "messages": messages
        }
        
    except HTTPException:
        # 重新抛出已定义的业务异常
        raise
    except pymysql.MySQLError as e:
        # 数据库异常回滚
        db.rollback()
        raise HTTPException(status_code=500, detail=f"消息记录写入失败：{str(e)}")
    except Exception as e:
        # 捕获所有其他异常，给出友好提示
        raise HTTPException(status_code=500, detail=f"消息推送失败：{str(e)}")
    finally:
        # 仅关闭游标，数据库连接由依赖管理
        if cursor:
            cursor.close()


@router.get(
    "/query",
    response_model=NotificationQueryResponse,
    summary="查看已推送消息",
    description="查看自己发送的消息，支持三种查询方式：1.按目标id查找 2.管理员查找 3.教师查找（三选一）"
)
def query_notifications(
    target_id: Optional[str] = Query(None, description="目标对象的ID（学生学号或教师工号）"),
    admin_id: Optional[str] = Query(None, description="管理员ID（自增id，仅管理员可用）"),
    teacher_id: Optional[str] = Query(None, description="教师工号（仅教师可用）"),
    status: Optional[str] = Query(None, description="按状态筛选：unread, read, retracted"),
    page: int = 1,
    page_size: int = 20,
    current_user: str = Query(..., description="当前用户信息(JSON字符串)，示例: {\"sub\":1,\"roles\":[\"teacher\"],\"username\":\"teacher1\"}"),
    db: pymysql.connections.Connection = Depends(get_db),
):
    # 1. 权限校验
    try:
        import urllib.parse
        current_user = urllib.parse.unquote(current_user)
        current_user_data = json.loads(current_user)
        user_roles = current_user_data.get("roles", [])
        user_sub = str(current_user_data.get("sub"))
        
        # 检查是否提供了有效的查询参数（三选一）
        query_params = [target_id, admin_id, teacher_id]
        if sum(1 for p in query_params if p) != 1:
            raise HTTPException(status_code=400, detail="必须提供且仅提供一个查询参数：target_id、admin_id或teacher_id")
            
    except json.JSONDecodeError:
        raise HTTPException(status_code=403, detail="无效的用户信息格式")
    except HTTPException:
        raise
    
    # 2. 分页参数校验
    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 20
    
    cursor = None
    try:
        cursor = db.cursor()
        # 3. 构建查询条件
        base_where = "1=1" 
        params = []
        
        # 基础条件：只能查看自己发送的消息
        # 使用更宽松的匹配方式，确保能找到消息
        base_where += " AND (metadata LIKE %s OR metadata IS NULL OR metadata = '{}')"
        params.append(f'%sender_id%{user_sub}%')
        
        # 处理查询参数
        if target_id:
            # 按目标id查找
            # 直接使用target_id作为user_id进行查询，不进行额外验证
            # 这样可以确保能找到推送时存储的消息
            base_where += " AND user_id = %s"
            params.append(target_id)
        
        elif admin_id:
            # 管理员查找
            if "admin" not in user_roles:
                raise HTTPException(status_code=403, detail="只有管理员可以使用admin_id参数")
            
            # 检查输入的admin_id是否与自身currentuser中的admins.id相符
            cursor.execute("SELECT id FROM admins WHERE id = %s", (admin_id,))
            admin_row = cursor.fetchone()
            if not admin_row:
                raise HTTPException(status_code=404, detail="管理员ID不存在")
            
            if str(admin_row[0]) != user_sub:
                raise HTTPException(status_code=403, detail="输入的管理员ID与当前用户不符")
        
        elif teacher_id:
            # 教师查找
            if "teacher" not in user_roles:
                raise HTTPException(status_code=403, detail="只有教师可以使用teacher_id参数")
            
            # 检查输入的teacher_id是否与自身currentuser中的teacher_id相符
            cursor.execute("SELECT id, teacher_id FROM teachers WHERE teacher_id = %s", (teacher_id,))
            teacher_row = cursor.fetchone()
            if not teacher_row:
                raise HTTPException(status_code=404, detail="教师工号不存在")
            
            # 获取教师的自增ID
            teacher_internal_id = str(teacher_row[0])
            if teacher_internal_id != user_sub:
                raise HTTPException(status_code=403, detail="输入的教师工号与当前用户不符")
        
        # 按状态筛选
        if status:
            base_where += " AND status = %s"
            params.append(status)
        
        # 4. 查询总记录数
        count_sql = f"SELECT COUNT(*) FROM user_messages WHERE {base_where}"
        cursor.execute(count_sql, params)
        total = cursor.fetchone()[0]
        
        # 5. 分页查询数据
        offset = (page - 1) * page_size
        select_sql = f"""
        SELECT id, user_id, username, title, content, source, status, received_time, metadata 
        FROM user_messages 
        WHERE {base_where} 
        ORDER BY received_time DESC 
        LIMIT %s OFFSET %s
        """
        cursor.execute(select_sql, params + [page_size, offset])
        rows = cursor.fetchall()
        
        # 6. 组装返回数据
        items = []
        for row in rows:
            # row结构：(id, user_id, username, title, content, source, status, received_time, metadata)
            try:
                metadata = json.loads(row[8]) if row[8] else {}
            except Exception:
                metadata = {}
            sender_id = metadata.get("sender_id")
            items.append(
                NotificationItem(
                    id=row[0],
                    user_id=row[1],
                    username=row[2] or "",
                    title=row[3],
                    content=row[4],
                    operation_time=row[7].strftime("%Y-%m-%d %H:%M:%S") if row[7] else None,
                    status=row[6],  # unread/read/retracted
                    sender_id=sender_id
                )
            )
        
        # 7. 计算总页数
        total_pages = (total + page_size - 1) // page_size
        return NotificationQueryResponse(
            items=items,
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
        )
    except HTTPException:
        raise
    except pymysql.MySQLError as e:
        raise HTTPException(status_code=500, detail=f"查询失败：{str(e)}")
    finally:
        if cursor:
            cursor.close()


@router.put(
    "/{notification_id}",
    summary="更新通知",
    description="更新已推送的通知内容，可修改标题和内容"
)
def update_notification(
    notification_id: int,
    payload: NotificationUpdate,
    db: pymysql.connections.Connection = Depends(get_db),
    # 可接入真实用户：current_user=Depends(get_current_user)
):
    cursor = None
    try:
        # 1. 核心参数校验
        if not (payload.title or payload.content):
            raise HTTPException(status_code=400, detail="至少需要提供标题或内容进行更新")
        
        cursor = db.cursor()
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        
        # 2. 检查通知是否存在
        cursor.execute("SELECT id FROM user_messages WHERE id = %s", (notification_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="通知不存在")
        
        # 3. 准备更新字段
        updates = []
        params = []
        
        if payload.title:
            updates.append("title = %s")
            params.append(payload.title)
        
        if payload.content:
            content_value = payload.content or ""
            # 处理长内容
            metadata = {}
            if len(content_value) > 60000:
                metadata["long_content"] = content_value[60000:]
                content_value = content_value[:60000]
            
            # 先获取现有metadata
            cursor.execute("SELECT metadata FROM user_messages WHERE id = %s", (notification_id,))
            existing_metadata = cursor.fetchone()[0]
            if existing_metadata:
                try:
                    existing_metadata = json.loads(existing_metadata)
                    # 合并现有metadata
                    existing_metadata.update(metadata)
                    metadata = existing_metadata
                except Exception:
                    pass
            
            updates.append("content = %s")
            params.append(content_value)
            updates.append("metadata = %s")
            params.append(json.dumps(metadata, ensure_ascii=False) if metadata else None)
        
        updates.append("updated_at = %s")
        params.append(now_str)
        params.append(notification_id)
        
        # 4. 执行更新
        update_sql = f"UPDATE user_messages SET {', '.join(updates)} WHERE id = %s"
        cursor.execute(update_sql, params)
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="通知更新失败")
        
        db.commit()
        
        # 5. 返回更新结果
        return {
            "message": "通知更新成功",
            "notification_id": notification_id
        }
        
    except HTTPException:
        raise
    except pymysql.MySQLError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"通知更新失败：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新处理失败：{str(e)}")
    finally:
        if cursor:
            cursor.close()


@router.put(
    "/{notification_id}/retract",
    summary="撤回通知",
    description="撤回已推送的通知，将状态标记为已撤回"
)
def retract_notification(
    notification_id: int,
    db: pymysql.connections.Connection = Depends(get_db),
    # 可接入真实用户：current_user=Depends(get_current_user)
):
    cursor = None
    try:
        cursor = db.cursor()
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        
        # 1. 检查通知是否存在
        cursor.execute("SELECT id FROM user_messages WHERE id = %s", (notification_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="通知不存在")
        
        # 2. 执行撤回操作（将状态改为已撤回）
        update_sql = """
        UPDATE user_messages 
        SET status = 'retracted', updated_at = %s 
        WHERE id = %s
        """
        cursor.execute(update_sql, (now_str, notification_id))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="通知撤回失败")
        
        db.commit()
        
        # 3. 返回撤回结果
        return {
            "message": "通知已成功撤回",
            "notification_id": notification_id
        }
        
    except HTTPException:
        raise
    except pymysql.MySQLError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"通知撤回失败：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"撤回处理失败：{str(e)}")
    finally:
        if cursor:
            cursor.close()


@router.get(
    "/received",
    summary="查看收到的消息",
    description="学生和教师查看发给自己的消息，返回消息的标题、内容、操作时间和发送者姓名"
)
def get_received_notifications(
    student_id: Optional[str] = Query(None, description="学生学号（仅学生可用）"),
    teacher_id: Optional[str] = Query(None, description="教师工号（仅教师可用）"),
    status: Optional[str] = Query(None, description="按状态筛选：unread, read, retracted"),
    page: int = 1,
    page_size: int = 20,
    current_user: str = Query(..., description="当前用户信息(JSON字符串)，示例: {\"sub\":1,\"roles\":[\"student\"],\"username\":\"student1\"}"),
    db: pymysql.connections.Connection = Depends(get_db),
):
    # 1. 权限校验
    try:
        import urllib.parse
        current_user = urllib.parse.unquote(current_user)
        current_user_data = json.loads(current_user)
        user_roles = current_user_data.get("roles", [])
        user_sub = str(current_user_data.get("sub"))
        
        # 检查是否提供了有效的查询参数（二选一）
        query_params = [student_id, teacher_id]
        if sum(1 for p in query_params if p) != 1:
            raise HTTPException(status_code=400, detail="必须提供且仅提供一个查询参数：student_id或teacher_id")
        
        # 验证权限
        if student_id:
            if "student" not in user_roles:
                raise HTTPException(status_code=403, detail="只有学生可以使用student_id参数")
            # 验证学生ID与当前用户是否匹配
            cursor = db.cursor()
            cursor.execute("SELECT id FROM students WHERE student_id = %s", (student_id,))
            student_row = cursor.fetchone()
            if not student_row:
                raise HTTPException(status_code=404, detail="学生学号不存在")
            if str(student_row[0]) != user_sub:
                raise HTTPException(status_code=403, detail="输入的学生学号与当前用户不符")
        
        if teacher_id:
            if "teacher" not in user_roles:
                raise HTTPException(status_code=403, detail="只有教师可以使用teacher_id参数")
            # 验证教师ID与当前用户是否匹配
            cursor = db.cursor()
            cursor.execute("SELECT id FROM teachers WHERE teacher_id = %s", (teacher_id,))
            teacher_row = cursor.fetchone()
            if not teacher_row:
                raise HTTPException(status_code=404, detail="教师工号不存在")
            if str(teacher_row[0]) != user_sub:
                raise HTTPException(status_code=403, detail="输入的教师工号与当前用户不符")
            
    except json.JSONDecodeError:
        raise HTTPException(status_code=403, detail="无效的用户信息格式")
    except HTTPException:
        raise
    
    # 2. 分页参数校验
    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 20
    
    cursor = None
    try:
        cursor = db.cursor()
        # 3. 构建查询条件
        base_where = "1=1" 
        params = []
        
        # 基础条件：只能查看发给自己的消息
        if student_id:
            base_where += " AND user_id = %s"
            params.append(student_id)
        elif teacher_id:
            base_where += " AND user_id = %s"
            params.append(teacher_id)
        
        # 按状态筛选
        if status:
            base_where += " AND status = %s"
            params.append(status)
        
        # 4. 查询总记录数
        count_sql = f"SELECT COUNT(*) FROM user_messages WHERE {base_where}"
        cursor.execute(count_sql, params)
        total = cursor.fetchone()[0]
        
        # 5. 分页查询数据
        offset = (page - 1) * page_size
        select_sql = f"""
        SELECT id, user_id, username, title, content, source, status, received_time, metadata 
        FROM user_messages 
        WHERE {base_where} 
        ORDER BY received_time DESC 
        LIMIT %s OFFSET %s
        """
        cursor.execute(select_sql, params + [page_size, offset])
        rows = cursor.fetchall()
        
        # 6. 组装返回数据
        items = []
        for row in rows:
            # row结构：(id, user_id, username, title, content, source, status, received_time, metadata)
            try:
                metadata = json.loads(row[8]) if row[8] else {}
            except Exception:
                metadata = {}
            sender_id = metadata.get("sender_id")
            sender_role = metadata.get("sender_role")
            
            # 获取发送者姓名
            sender_name = ""
            if sender_id and sender_role:
                if sender_role == "admin":
                    # 查询管理员姓名
                    cursor.execute("SELECT name FROM admins WHERE id = %s", (sender_id,))
                    admin_row = cursor.fetchone()
                    if admin_row:
                        sender_name = admin_row[0]
                elif sender_role == "teacher":
                    # 查询教师姓名
                    cursor.execute("SELECT name FROM teachers WHERE id = %s", (sender_id,))
                    teacher_row = cursor.fetchone()
                    if teacher_row:
                        sender_name = teacher_row[0]
            
            items.append({
                "title": row[3],
                "content": row[4],
                "operation_time": row[7].strftime("%Y-%m-%d %H:%M:%S") if row[7] else None,
                "sender_name": sender_name
            })
        
        # 7. 计算总页数
        total_pages = (total + page_size - 1) // page_size
        return {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages
        }
    except HTTPException:
        raise
    except pymysql.MySQLError as e:
        raise HTTPException(status_code=500, detail=f"查询失败：{str(e)}")
    finally:
        if cursor:
            cursor.close()
