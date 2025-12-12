from flask import Flask, render_template, request, session, redirect, url_for, flash
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_limiter.errors import RateLimitExceeded
import os
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash


app = Flask(__name__)

db_host = os.getenv('BLOG_DB_HOST')
db_name = os.getenv('BLOG_DB_NAME')
db_user = os.getenv('BLOG_DB_USER')
db_password = os.getenv('BLOG_DB_PASSWORD')

app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

app.secret_key = os.getenv('BLOG_FLASK_APP_SECRET_KEY')
APP_PASSWORD_HASHED = os.getenv('BLOG_PASSWORD_HASH')

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["100 per day"]
)

@app.errorhandler(RateLimitExceeded)
def ratelimit_handler(e):
    # Проверяем путь запроса
    if request.path == '/login' and request.method == 'POST':
        flash('Слишком много попыток входа. Попробуйте позже.')
        return redirect(request.referrer or url_for('index'))
    
    return "Too Many Requests", 429


class Category(db.Model):
    __tablename__ = 'category'
    
    category_id = db.Column(db.Integer, primary_key=True)
    category_name = db.Column(db.String(100), unique=True, nullable=False)
    
    # Связь с хостингами
    hostings = db.relationship('Hosting', secondary='hosting_category', back_populates='categories')


class Hosting(db.Model):
    __tablename__ = 'hosting'
    
    hosting_id = db.Column(db.Integer, primary_key=True)
    hosting_name = db.Column(db.String(500), unique=True, nullable=False)
    url = db.Column(db.String(1000))
    status = db.Column(db.String(500))
    risk = db.Column(db.Integer)
    advantages = db.Column(db.Text)
    disadvantages = db.Column(db.Text)
    hosting_location = db.Column(db.String(500))
    servers_location = db.Column(db.String(500))
    min_price_in_dollars = db.Column(db.Float)
    favorite = db.Column(db.Boolean, default=False)
    
    # Связь с категориями
    categories = db.relationship('Category', secondary='hosting_category', back_populates='hostings')

    @property
    def status_class(self):
        if not self.status:
            return ""
        
        status_lower = self.status.lower()
        
        if status_lower == "ok":
            return "ok"
        elif "ok" in status_lower:
            return "contains_ok"
        elif "kyc" in status_lower:
            return "kyc"
        
        return ""


# Таблица многие-ко-многим
class HostingCategory(db.Model):
    __tablename__ = 'hosting_category'
    
    hosting_id = db.Column(db.Integer, db.ForeignKey('hosting.hosting_id'), primary_key=True)
    category_id = db.Column(db.Integer, db.ForeignKey('category.category_id'), primary_key=True)


# Функция для получения хостингов с фильтрами
def get_filtered_hostings(**filters):
    query = Hosting.query
    
    # Применяем фильтры
    if filters.get('category_id'):
        query = query.join(HostingCategory).filter(HostingCategory.category_id == filters['category_id'])
    
    if filters.get('max_price') is not None:
        query = query.filter(Hosting.min_price_in_dollars <= filters['max_price'])
    
    if filters.get('hosting_name') is not None:
        query = query.filter(Hosting.hosting_name.ilike(f'%{filters["hosting_name"]}%'))
    
    if filters.get('favorite'):
        query = query.filter(Hosting.favorite == True)
    
    if filters.get('status'):
        query = query.filter(Hosting.status.ilike(f'%{filters["status"]}%'))
    
    if filters.get('max_risk') is not None:
        query = query.filter(Hosting.risk <= filters['max_risk'])
    
    if filters.get('location'):
        query = query.filter(
            or_(
                Hosting.hosting_location.ilike(f'%{filters["location"]}%'),
                Hosting.servers_location.ilike(f'%{filters["location"]}%')
            )
        )

    # Сортировка
    sort_by = filters.get('sort_by', 'hosting_name')
    sort_order = filters.get('sort_order', 'asc')
    
    if hasattr(Hosting, sort_by):
        column = getattr(Hosting, sort_by)
        if sort_order == 'desc':
            query = query.order_by(desc(column))
        else:
            query = query.order_by(column)
    else:
        # Сортировка по умолчанию
        query = query.order_by(Hosting.hosting_name)

    return query


@app.route('/login', methods=['POST'])
@limiter.limit("5 per minute;20 per hour")
def login():
    # Получаем URL, с которого пришел запрос (текущая страница)
    referrer = request.referrer or url_for('index')
    
    # if request.form.get('password') == APP_PASSWORD:
    # APP_PASSWORD_HASHED = "scrypt:32768:8:1$z3kOQDcsAgbdxFK0$966fe1fd349be9618a7a1af5569f9fa91b7d27fb4c9486ec084175ad2656108edbc331f34d02a66b062070a1e28de84905acd00e2dfbc63b5f34b44a04489222"
    # print(APP_PASSWORD_HASHED)
    if check_password_hash(APP_PASSWORD_HASHED, request.form.get('password', '')):
        session['authenticated'] = True
        # Возвращаем на ту же страницу, откуда пришли
        return redirect(referrer)
    else:
        flash('Неверный пароль')
        # Возвращаем на ту же страницу с сообщением об ошибке
        return redirect(referrer)


@app.route('/logout')
def logout():
    session.pop('authenticated', None)
    flash('Вы вышли из системы')
    return redirect(request.referrer or url_for('index'))


@app.route('/xray_client_setup')
def xray_client_setup():
    return render_template('xray_client_setup.html')


@app.route('/xray_server_setup')
def xray_server_setup():
    return render_template('xray_server_setup.html')


@app.route('/hostings_list')
def hostings():
    categories = Category.query.all()
    return render_template('hostings_list.html', categories=categories)


@app.route('/hostings_table')
def show_hostings_table():
    # Собираем параметры фильтрации
    category_id = request.args.get('category', type=int)
    max_price = request.args.get('max_price', type=float)
    hosting_name = request.args.get('hosting_name')
    favorite = request.args.get('favorite')
    status = request.args.get('status')
    max_risk = request.args.get('max_risk', type=int)
    location = request.args.get('location')
    sort_by = request.args.get('sort_by', 'hosting_name')
    sort_order = request.args.get('sort_order', 'asc')

    # Строим запрос
    query = get_filtered_hostings(
        category_id=category_id,
        max_price=max_price,
        hosting_name=hosting_name,
        favorite=favorite,
        status=status,
        max_risk=max_risk,
        location=location,
        sort_by=sort_by,
        sort_order=sort_order
    )
    
    hostings = query.all()
    return render_template('hostings_table.html', hostings=hostings)


@app.route('/')
def index():
    return render_template('index.html')