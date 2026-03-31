def get_custom_css() -> str:
    return """
    <style>
    @keyframes gradient-flow {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    /* Основной фон для всего приложения */
    .stApp {
        background: linear-gradient(-45deg, #85ACE5, #1148A8, #4A6FA5, #0E3A78);
        background-size: 400% 400%;
        animation: gradient-flow 15s ease infinite;
    }
    
    /* Скрыть фон и границы header, но оставить кнопки */
    header[data-testid="stHeader"] {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        min-height: 0 !important;
        height: auto !important;
    }

    
    /* Фон для main контейнера */
    .main {
        background: transparent !important;
    }
    
    /* Фон для body */
    body {
        background: transparent !important;
    }
    
    /* Убираем стандартный фон у контента */
    .block-container {
        background: transparent !important;
    }
    
    .page-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .page-header h1 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: 700;
    }
    
    .page-header p {
        margin: 0.5rem 0 0 0;
        opacity: 0.95;
        font-size: 1.1rem;
    }
    
    [data-testid="stSidebar"] {
        background-color: rgba(26, 26, 46, 0.95) !important;
        backdrop-filter: blur(10px);
        color: #ffffff !important;
    }
    
    [data-testid="stSidebar"] * {
        color: #ffffff !important;
    }
    
    [data-testid="stSidebar"] .stMarkdown {
        color: #ffffff !important;
    }
    
    [data-testid="stSidebar"] h1, 
    [data-testid="stSidebar"] h2, 
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] h4,
    [data-testid="stSidebar"] h5,
    [data-testid="stSidebar"] h6 {
        color: #ffffff !important;
    }
    
    [data-testid="stSidebar"] p, 
    [data-testid="stSidebar"] span, 
    [data-testid="stSidebar"] div {
        color: #e0e0e0 !important;
    }
    
    [data-testid="stSidebar"] a {
        color: #667eea !important;
    }
    
    [data-testid="stSidebar"] a:hover {
        color: #764ba2 !important;
    }
    
    [data-testid="stSidebar"] [data-baseweb="menu"] {
        background-color: #16213e !important;
    }
    
    [data-testid="stSidebar"] [data-baseweb="menu-item"] {
        color: #ffffff !important;
    }
    
    [data-testid="stSidebar"] [data-baseweb="menu-item"]:hover {
        background-color: #667eea !important;
        color: #ffffff !important;
    }
    
    [data-testid="stSidebar"] hr {
        border-color: #333355 !important;
    }
    
    [data-testid="stSidebar"] .stAlert {
        background-color: #16213e !important;
        border: 1px solid #667eea !important;
    }
    
    [data-testid="stSidebar"] .stAlert p {
        color: #ffffff !important;
    }
    
    .welcome-banner {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 2rem;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .welcome-banner h2 {
        margin: 0 0 0.5rem 0;
        font-size: 2rem;
    }
    
    .welcome-banner p {
        margin: 0;
        opacity: 0.95;
        font-size: 1.1rem;
    }
    
    [data-testid="stMetric"] {
        background-color: rgba(255, 255, 255, 0.95);
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #667eea;
    }
    
    [data-testid="stMetricLabel"] {
        color: #374151 !important;
        font-weight: 600;
    }
    
    [data-testid="stMetricValue"] {
        color: #667eea !important;
        font-weight: 700;
    }
    
    [data-testid="stMetricDelta"] {
        color: #10b981 !important;
    }
    
    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white !important;
        border: none;
        border-radius: 6px;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
    
    .vacancy-card, .resume-card {
        background-color: rgba(255, 255, 255, 0.95);
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #667eea;
        transition: all 0.3s ease;
    }
    
    .vacancy-card:hover, .resume-card:hover {
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        transform: translateY(-2px);
    }
    
    .activity-section {
        background-color: rgba(255, 255, 255, 0.95);
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .activity-item {
        display: flex;
        align-items: center;
        padding: 1rem;
        border-bottom: 1px solid #e0e0e0;
    }
    
    .activity-item:last-child {
        border-bottom: none;
    }
    
    .activity-icon {
        font-size: 1.5rem;
        margin-right: 1rem;
    }
    
    .activity-text {
        flex-grow: 1;
        color: #374151;
    }
    
    .activity-time {
        color: #9ca3af;
        font-size: 0.875rem;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
        background-color: rgba(255, 255, 255, 0.95);
        padding: 0.5rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
        border-radius: 6px;
        color: #374151 !important;
        font-weight: 500;
    }
    
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white !important;
    }
    
    .stTextInput>div>div>input,
    .stTextArea>div>div>textarea,
    .stSelectbox>div>div>select,
    .stNumberInput>div>div>input {
        background-color: white;
        border: 1px solid #d1d5db;
        border-radius: 6px;
        color: #374151;
    }
    
    .stTextInput>div>div>input:focus,
    .stTextArea>div>div>textarea:focus {
        border-color: #667eea;
        box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
    }
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    [data-testid="stDecoration"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    
    .skill-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        margin: 0.25rem;
        border-radius: 20px;
        font-size: 0.875rem;
        font-weight: 500;
        background-color: #667eea20;
        border: 1px solid #667eea;
        color: #667eea;
    }
    
    .match-score {
        display: flex;
        align-items: center;
        gap: 1rem;
        padding: 1rem;
        border-radius: 8px;
        background-color: #f9fafb;
    }
    
    .match-score-circle {
        width: 60px;
        height: 60px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-weight: 700;
        font-size: 1.2rem;
    }
    
    .match-score-text {
        font-weight: 600;
    }
    
    .placeholder-box {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        padding: 3rem;
        border-radius: 10px;
        text-align: center;
        color: #6b7280;
    }
    
    .placeholder-box p {
        margin: 0.5rem 0;
    }
    </style>
    """