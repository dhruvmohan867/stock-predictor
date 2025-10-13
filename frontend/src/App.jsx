import { useState, useEffect } from 'react';
import AuthForm from './components/AuthForm';
import Dashboard from './components/Dashboard';

function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  // On initial load, check if a token exists in local storage
  useEffect(() => {
    if (localStorage.getItem('token')) {
      setIsAuthenticated(true);
    }
  }, []);

  const handleLogin = () => {
    setIsAuthenticated(true);
  };

  const handleLogout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('username');
    setIsAuthenticated(false);
  };

  // If authenticated, render the main dashboard
  if (isAuthenticated) {
    return <Dashboard onLogout={handleLogout} />;
  }

  // Otherwise, render the beautiful new authentication screen
  return (
    <div className="min-h-screen w-full bg-gray-900 flex">
      {/* Left Panel: Your Image */}
      <div 
        className="hidden lg:block lg:w-1/2 bg-cover bg-center"
        // --- IMAGE SLOT ---
        // 1. Add an image (e.g., `auth-bg.jpg`) to your `frontend/public` folder.
        // 2. The line below will then display it automatically.
        style={{ backgroundImage: "url(https://miro.medium.com/v2/resize:fit:786/format:webp/1*xBuJWs9kO51tyvhrhc9JYA.jpeg)" }}
      >
        {/* This div is purely for the background image */}
      </div>

      {/* Right Panel: The Authentication Form */}
      <div className="w-full lg:w-1/2 flex items-center justify-center p-8">
        <AuthForm onLogin={handleLogin} />
      </div>
    </div>
  );
}

export default App;