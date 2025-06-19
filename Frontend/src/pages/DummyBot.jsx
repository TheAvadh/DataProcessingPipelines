import { useState, useEffect, useRef } from 'react'
import { MessageCircle, Send, X } from 'lucide-react'

export default function Component() {
  const [isChatOpen, setIsChatOpen] = useState(false)
  const [messages, setMessages] = useState([])
  const [inputMessage, setInputMessage] = useState('')
  const [isAssistantTyping, setIsAssistantTyping] = useState(false)
  const messagesEndRef = useRef(null)

  const toggleChat = () => setIsChatOpen(!isChatOpen)

  const handleSendMessage = (e) => {
    e.preventDefault()
    if (inputMessage.trim()) {
      setMessages([...messages, { text: inputMessage, isUser: true }])
      setInputMessage('')
      setIsAssistantTyping(true)

      setTimeout(() => {
        setIsAssistantTyping(false)
        setMessages(prev => [...prev, { text: "Hello! I'm your virtual assistant. How can I help you today?", isUser: false }])
      }, 1500)
    }
  }

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  return (
    <div className="min-vh-100 bg-light">
      {/* Chatbot Icon */}
      <button
        onClick={toggleChat}
        className="position-fixed bottom-0 end-0 m-4 btn btn-primary rounded-circle shadow-lg"
        aria-label={isChatOpen ? "Close chat" : "Open chat"}
      >
        <MessageCircle size={30} color="white" />
      </button>

      {/* Chat Interface */}
      <div 
        className={`position-fixed bottom-10 end-0 m-4 bg-white rounded shadow-lg d-flex flex-column ${
          isChatOpen ? 'opacity-100' : 'opacity-0 d-none'
        }`}
        style={{ width: '320px', height: 'calc(100vh - 100px)', maxHeight: '600px', transition: 'opacity 0.3s ease' }}
      >
        <div className="d-flex justify-content-between align-items-center p-3 bg-primary text-white rounded-top">
          <h5 className="mb-0">Virtual Assistant</h5>
          <button 
            onClick={toggleChat} 
            className="btn text-white"
            aria-label="Close chat"
          >
            <X size={20} color="white" />
          </button>
        </div>
        <div className="flex-grow-1 overflow-auto p-3">
          {messages.map((message, index) => (
            <div key={index} className={`d-flex ${message.isUser ? 'justify-content-end' : 'justify-content-start'} mb-2`}>
              <div 
                className={`p-2 rounded ${message.isUser ? 'bg-primary text-white' : 'bg-light text-dark'}`}
                style={{ maxWidth: '70%' }}
              >
                {message.text}
              </div>
            </div>
          ))}
          {isAssistantTyping && (
            <div className="d-flex justify-content-start mb-2">
              <div className="bg-light p-2 rounded">
                <span className="spinner-border spinner-border-sm text-primary" role="status" aria-hidden="true"></span>
                <span className="ms-2">Typing...</span>
              </div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>
        <form onSubmit={handleSendMessage} className="d-flex border-top p-3">
          <input
            type="text"
            value={inputMessage}
            onChange={(e) => setInputMessage(e.target.value)}
            placeholder="Type your message..."
            className="form-control rounded-start"
          />
          <button
            type="submit"
            className="btn btn-primary rounded-end"
            aria-label="Send message"
          >
            <Send size={20} color="white" />
          </button>
        </form>
      </div>
    </div>
  )
}

