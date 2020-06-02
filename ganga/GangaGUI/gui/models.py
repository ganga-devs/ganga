import datetime
import jwt
from GangaGUI.gui import db, app
from werkzeug.security import generate_password_hash, check_password_hash


# ORM Class to represent Users - used to access the GUI & API resources
class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    public_id = db.Column(db.String(64), unique=True)
    user = db.Column(db.String(32))
    password_hash = db.Column(db.String(64))
    role = db.Column(db.String(32))

    def store_password_hash(self, password: str):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def generate_auth_token(self, expires_in_days: int = 5) -> str:
        return jwt.encode({"public_id": self.public_id, "exp": datetime.datetime.utcnow() + datetime.timedelta(days=expires_in_days)},
                          app.config["SECRET_KEY"], algorithm="HS256")

    def __repr__(self):
        return "User {}: {} (Public ID: {}, Role: {})".format(self.id, self.user, self.public_id, self.role)
