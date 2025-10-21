import { useState, useEffect } from 'react';
import { Loader, Eye, EyeOff } from 'lucide-react';
import { motion } from 'framer-motion';

const API_BASE = import.meta.env.VITE_API_BASE || "https://stock-predictor-ujiu.onrender.com";
const GOOGLE_CLIENT_ID = import.meta.env.VITE_GOOGLE_CLIENT_ID;

const AuthForm = ({ onLogin }) => {
  const [isLogin, setIsLogin] = useState(true);
  const [username, setUsername] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  // --- Google Sign-In callback ---
  const handleGoogleCredential = async (response) => {
    try {
      const res = await fetch(`${API_BASE}/google-login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ credential: response.credential }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || 'Google sign-in failed');

      localStorage.setItem('token', data.access_token);
      localStorage.setItem('username', data.username);
      onLogin();
    } catch (err) {
      setError(err.message);
    }
  };

  // --- Load Google Sign-In script dynamically ---
  useEffect(() => {
    const loadGoogleScript = () => {
      if (document.getElementById('google-client-script')) {
        // Script already loaded, just initialize button
        if (window.google && GOOGLE_CLIENT_ID) {
          window.google.accounts.id.initialize({
            client_id: GOOGLE_CLIENT_ID,
            callback: handleGoogleCredential,
          });
          window.google.accounts.id.renderButton(
            document.getElementById('googleSignInDiv'),
            { theme: 'filled_blue', size: 'large', shape: 'rectangular', width: 250 }
          );
        }
        return;
      }

      const script = document.createElement('script');
      script.src = 'https://accounts.google.com/gsi/client';
      script.id = 'google-client-script';
      script.async = true;
      script.defer = true;
      script.onload = () => {
        if (window.google && GOOGLE_CLIENT_ID) {
          window.google.accounts.id.initialize({
            client_id: GOOGLE_CLIENT_ID,
            callback: handleGoogleCredential,
          });
          window.google.accounts.id.renderButton(
            document.getElementById('googleSignInDiv'),
            { theme: 'filled_blue', size: 'large', shape: 'rectangular', width: 250 }
          );
        }
      };
      document.body.appendChild(script);
    };

    loadGoogleScript();
  }, []);

  // --- Handle login/register ---
  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      if (isLogin) {
        // LOGIN
        const formData = new FormData();
        formData.append('username', username);
        formData.append('password', password);

        const response = await fetch(`${API_BASE}/token`, {
          method: 'POST',
          body: formData,
        });

        const data = await response.json();
        if (!response.ok) throw new Error(data.detail || 'Login failed');

        localStorage.setItem('token', data.access_token);
        localStorage.setItem('username', username);
        onLogin();
      } else {
        // REGISTER
        const response = await fetch(`${API_BASE}/register`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ username, email, password }),
        });

        const data = await response.json();
        if (!response.ok) throw new Error(data.detail || 'Registration failed');

        // Auto-login
        const loginForm = new FormData();
        loginForm.append('username', username);
        loginForm.append('password', password);

        const loginResponse = await fetch(`${API_BASE}/token`, {
          method: 'POST',
          body: loginForm,
        });

        if (loginResponse.ok) {
          const loginData = await loginResponse.json();
          localStorage.setItem('token', loginData.access_token);
          localStorage.setItem('username', username);
          onLogin();
        } else {
          setError('Account created! Please sign in.');
          setIsLogin(true);
        }
      }
    } catch (err) {
      console.error('Auth error:', err);
      setError(err.message || 'Something went wrong');
    } finally {
      setLoading(false);
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: -20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5 }}
      className="w-full max-w-sm text-gray-300"
    >
      <div className="text-left mb-10">
        <h1 className="text-3xl font-bold text-white">
          {isLogin ? 'Welcome Back 👋' : 'Create an Account'}
        </h1>
        <p className="mt-2 text-gray-400">
          {isLogin
            ? 'Please login with your details here'
            : 'Get started with your free account'}
        </p>
      </div>

      <form onSubmit={handleSubmit} className="space-y-5">
        <div>
          <label className="text-xs font-medium text-gray-400">Username</label>
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            className="w-full mt-1 px-4 py-3 bg-gray-800 border border-gray-700 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
            placeholder="Enter your username"
            required
          />
        </div>

        {!isLogin && (
          <div>
            <label className="text-xs font-medium text-gray-400">Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full mt-1 px-4 py-3 bg-gray-800 border border-gray-700 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
              placeholder="your@email.com"
              required
            />
          </div>
        )}

        <div>
          <label className="text-xs font-medium text-gray-400">Password</label>
          <div className="relative mt-1">
            <input
              type={showPassword ? 'text' : 'password'}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full px-4 py-3 bg-gray-800 border border-gray-700 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
              placeholder="Enter your password"
              required
            />
            <button
              type="button"
              onClick={() => setShowPassword(!showPassword)}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-300"
            >
              {showPassword ? <EyeOff size={20} /> : <Eye size={20} />}
            </button>
          </div>
        </div>

        {error && (
          <p className={`text-sm ${error.includes('success') ? 'text-green-400' : 'text-red-400'}`}>
            {error}
          </p>
        )}

        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          type="submit"
          disabled={loading}
          className="w-full bg-blue-500 text-white py-3 rounded-lg font-semibold hover:bg-blue-600 transition disabled:opacity-50"
        >
          {loading ? <Loader className="animate-spin mx-auto" /> : isLogin ? 'Sign In' : 'Create Account'}
        </motion.button>
      </form>

      <div className="mt-6 flex justify-center">
        <div id="googleSignInDiv" />
      </div>

      <div className="text-center mt-8 text-sm">
        <p>
          {isLogin ? "Don't have an account?" : 'Already have an account?'}
          <button
            onClick={() => {
              setIsLogin(!isLogin);
              setError('');
            }}
            className="font-semibold text-blue-400 hover:underline ml-2"
          >
            {isLogin ? 'Sign Up' : 'Sign In'}
          </button>
        </p>
      </div>
    </motion.div>
  );
};

export default AuthForm;
